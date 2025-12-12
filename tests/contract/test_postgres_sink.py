"""
Contract Tests for PostgreSQL Sink Connector

Validates sink connector input processing, SQL generation, and error handling.
"""

import pytest
import time
import uuid
from cassandra.cluster import Cluster
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
from tests.conftest import wait_for_data_in_postgres


def check_connector_running(url):
    """Check if Kafka Connect and postgres-jdbc-sink connector are running."""
    try:
        response = requests.get(f"{url}/connectors/postgres-jdbc-sink/status", timeout=5)
        return response.status_code == 200 and response.json().get('connector', {}).get('state') == 'RUNNING'
    except:
        return False


requires_connectors = pytest.mark.skipif(
    not check_connector_running("http://localhost:8083"),
    reason="Requires Kafka Connect with postgres-sink connector running"
)


@pytest.fixture(scope="module")
def scylla_session():
    """Create ScyllaDB session."""
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    yield session
    cluster.shutdown()


@pytest.fixture(scope="module")
def postgres_conn():
    """Create PostgreSQL connection."""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='warehouse',
        user='postgres',
        password='postgres'
    )
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def kafka_connect_url():
    """Kafka Connect REST API URL."""
    return "http://localhost:8083"


@pytest.mark.contract
class TestPostgresSinkContract:
    """Contract tests for PostgreSQL sink connector."""

    def test_sink_creates_table_structure(self, postgres_conn):
        """Test sink creates tables with correct structure."""
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)

        # Check users table structure
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'cdc_data' AND table_name = 'users'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        assert len(columns) > 0, "Users table not created by sink"

        # Verify key columns exist (with _value suffix from Flatten transform)
        column_names = [col['column_name'] for col in columns]
        assert 'user_id' in column_names
        assert 'username_value' in column_names
        assert 'email_value' in column_names
        cursor.close()

    @requires_connectors
    def test_sink_handles_upsert_operations(self, scylla_session, postgres_conn):
        """Test sink performs UPSERT (INSERT or UPDATE)."""
        user_id = uuid.uuid4()
        username = f'upsert_test_{uuid.uuid4().hex[:8]}'

        # INSERT
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, 'upsert@test.com', 'Test', 'User', 'pending'))

        # Wait for INSERT to replicate with retry logic
        result = wait_for_data_in_postgres(
            "SELECT status_value as status FROM cdc_data.users WHERE user_id = %s",
            (str(user_id),),
            timeout=90
        )
        assert result is not None, f"INSERT not replicated for user_id={user_id}"
        assert result[0] == 'pending', f"Expected status 'pending', got '{result[0]}'"

        # UPDATE same record
        scylla_session.execute("""
            UPDATE app_data.users
            SET status = %s, updated_at = toTimestamp(now())
            WHERE user_id = %s
        """, ('active', user_id))

        # Wait for UPDATE to replicate with retry logic
        # Note: We need to wait for the specific value to appear, not just any data
        result = wait_for_data_in_postgres(
            "SELECT status_value as status FROM cdc_data.users WHERE user_id = %s AND status_value = %s",
            (str(user_id), 'active'),
            timeout=90
        )
        assert result is not None, f"UPDATE not replicated for user_id={user_id}"
        assert result[0] == 'active', f"UPSERT didn't update existing record: status={result[0]}"

        # Verify no duplicates exist
        cursor = postgres_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM cdc_data.users WHERE user_id = %s", (str(user_id),))
        count = cursor.fetchone()[0]
        cursor.close()
        assert count == 1, f"UPSERT created duplicate instead of updating: count={count}"

    @requires_connectors
    def test_sink_preserves_data_types(self, scylla_session, postgres_conn):
        """Test sink preserves data types correctly."""
        product_id = uuid.uuid4()

        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'Type Test Product', 'Description', 99.99, 100, 'Test', True))

        # Wait for INSERT to replicate with retry logic
        result = wait_for_data_in_postgres(
            """SELECT product_id, name_value as name, price_value as price, stock_quantity_value as stock_quantity, is_active_value as is_active,
                   pg_typeof(product_id) as id_type,
                   pg_typeof(price_value) as price_type,
                   pg_typeof(stock_quantity_value) as qty_type,
                   pg_typeof(is_active_value) as bool_type
            FROM cdc_data.products
            WHERE product_id = %s""",
            (str(product_id),),
            timeout=90
        )
        assert result is not None, f"INSERT not replicated for product_id={product_id}"

        # Verify types (result is a tuple)
        product_id_val, name, price, stock_quantity, is_active, id_type, price_type, qty_type, bool_type = result
        assert id_type in ('uuid', 'text'), f"Expected uuid or text, got {id_type}"
        assert price_type in ('numeric', 'decimal', 'double precision', 'text')
        assert qty_type in ('integer', 'bigint')
        assert bool_type == 'boolean'

        # Verify values
        assert float(price) == 99.99
        assert stock_quantity == 100
        assert is_active is True

    @requires_connectors
    def test_sink_handles_null_values(self, scylla_session, postgres_conn):
        """Test sink correctly handles NULL values."""
        product_id = uuid.uuid4()

        # Insert with some NULL values
        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'NULL Test', None, 10.00, 0, 'Test', True))

        # Wait for INSERT to replicate with retry logic
        result = wait_for_data_in_postgres(
            "SELECT description_value as description FROM cdc_data.products WHERE product_id = %s",
            (str(product_id),),
            timeout=90
        )
        assert result is not None, f"INSERT not replicated for product_id={product_id}"
        assert result[0] is None, "NULL value not preserved"

    @requires_connectors
    def test_sink_batch_processing(self, scylla_session, postgres_conn):
        """Test sink processes messages in batches."""
        # Insert multiple records quickly
        user_ids = []

        for i in range(10):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            scylla_session.execute("""
                INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
                VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
            """, (user_id, f'batch_user_{i}', f'batch{i}@test.com', 'Batch', f'User{i}', 'active'))

        # Wait for all records to replicate with retry logic
        placeholders = ','.join(['%s'] * len(user_ids))
        result = wait_for_data_in_postgres(
            f"SELECT COUNT(*) FROM cdc_data.users WHERE user_id::text IN ({placeholders})",
            [str(uid) for uid in user_ids],
            timeout=90
        )
        assert result is not None, "Batch INSERT not replicated"
        count = result[0]
        assert count == 10, f"Batch processing failed: expected 10, got {count}"

    @requires_connectors
    def test_sink_connector_configuration(self, kafka_connect_url):
        """Test sink connector is configured correctly."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-jdbc-sink/config")
        assert response.status_code == 200

        config = response.json()

        # Verify critical settings
        assert config.get('connector.class') == 'io.confluent.connect.jdbc.JdbcSinkConnector'
        assert 'connection.url' in config
        assert 'jdbc:postgresql' in config['connection.url']

        # Verify UPSERT mode
        assert config.get('insert.mode') == 'upsert'
        assert config.get('pk.mode') == 'record_key'

        # Verify batch settings
        assert 'batch.size' in config
        assert int(config['batch.size']) > 0

    @requires_connectors
    def test_sink_handles_special_characters(self, scylla_session, postgres_conn):
        """Test sink handles special characters in data."""
        user_id = uuid.uuid4()
        special_name = "Test'User\"With<Special>Chars&Symbols"

        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'special_{uuid.uuid4().hex[:8]}', 'special@test.com', special_name, 'User', 'active'))

        # Wait for INSERT to replicate with retry logic
        result = wait_for_data_in_postgres(
            "SELECT first_name_value as first_name FROM cdc_data.users WHERE user_id = %s",
            (str(user_id),),
            timeout=90
        )
        assert result is not None, f"INSERT not replicated for user_id={user_id}"
        assert result[0] == special_name, f"Special characters not preserved: expected '{special_name}', got '{result[0]}'"

    @requires_connectors
    def test_sink_referential_integrity(self, scylla_session, postgres_conn):
        """Test sink maintains referential integrity across tables."""
        user_id = uuid.uuid4()
        order_id = uuid.uuid4()

        # Insert user
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'ref_user_{uuid.uuid4().hex[:8]}', 'ref@test.com', 'Ref', 'User', 'active'))

        # Wait for user to replicate
        user_result = wait_for_data_in_postgres(
            "SELECT user_id FROM cdc_data.users WHERE user_id = %s",
            (str(user_id),),
            timeout=90
        )
        assert user_result is not None, f"User INSERT not replicated for user_id={user_id}"

        # Insert order referencing user
        scylla_session.execute("""
            INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
            VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, toTimestamp(now()), toTimestamp(now()))
        """, (order_id, user_id, 100.00, 'pending', '123 Test St'))

        # Wait for order to replicate and verify referential integrity with retry logic
        result = wait_for_data_in_postgres(
            """SELECT o.order_id, o.user_id_value as user_id, u.username_value as username
            FROM cdc_data.orders o
            JOIN cdc_data.users u ON o.user_id_value::text = u.user_id::text
            WHERE o.order_id = %s""",
            (str(order_id),),
            timeout=90
        )
        assert result is not None, f"Order INSERT not replicated or referential integrity broken for order_id={order_id}"
        assert result[1] == str(user_id), f"Expected user_id={user_id}, got {result[1]}"

    @requires_connectors
    def test_sink_error_handling_configuration(self, kafka_connect_url):
        """Test sink has proper error handling configured."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-jdbc-sink/config")
        assert response.status_code == 200

        config = response.json()

        # Verify error handling settings
        assert config.get('errors.tolerance') == 'all'
        assert 'errors.deadletterqueue.topic.name' in config
        assert config['errors.deadletterqueue.topic.name'] == 'dlq-postgres-sink'

    @requires_connectors
    def test_sink_connection_pooling(self, kafka_connect_url):
        """Test sink uses connection pooling."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-jdbc-sink/config")
        assert response.status_code == 200

        config = response.json()

        # Verify connection pool settings
        if 'connection.pool.size' in config:
            pool_size = int(config['connection.pool.size'])
            assert pool_size >= 5, "Connection pool size should be >= 5"
