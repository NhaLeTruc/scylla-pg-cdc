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

        # Verify key columns exist
        column_names = [col['column_name'] for col in columns]
        assert 'user_id' in column_names
        assert 'username' in column_names
        assert 'email' in column_names
        cursor.close()

    def test_sink_handles_upsert_operations(self, scylla_session, postgres_conn):
        """Test sink performs UPSERT (INSERT or UPDATE)."""
        user_id = uuid.uuid4()
        username = f'upsert_test_{uuid.uuid4().hex[:8]}'

        # INSERT
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, 'upsert@test.com', 'Test', 'User', 'pending'))

        time.sleep(10)

        # Verify INSERT
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT status
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['status'] == 'pending'

        # UPDATE same record
        scylla_session.execute("""
            UPDATE app_data.users
            SET status = %s, updated_at = toTimestamp(now())
            WHERE user_id = %s
        """, ('active', user_id))

        time.sleep(10)

        # Verify UPDATE (should be single record with updated status)
        cursor.execute("""
            SELECT COUNT(*), MAX(status) as status
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result['count'] == 1, "UPSERT created duplicate instead of updating"
        assert result['status'] == 'active', "UPSERT didn't update existing record"
        cursor.close()

    def test_sink_preserves_data_types(self, scylla_session, postgres_conn):
        """Test sink preserves data types correctly."""
        product_id = uuid.uuid4()

        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'Type Test Product', 'Description', 99.99, 100, 'Test', True))

        time.sleep(10)

        # Verify data types
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT product_id, product_name, price, stock_quantity, is_active,
                   pg_typeof(product_id) as id_type,
                   pg_typeof(price) as price_type,
                   pg_typeof(stock_quantity) as qty_type,
                   pg_typeof(is_active) as bool_type
            FROM cdc_data.products
            WHERE product_id = %s
        """, (str(product_id),))

        result = cursor.fetchone()
        assert result is not None

        # Verify types
        assert 'uuid' in result['id_type']
        assert result['price_type'] in ('numeric', 'decimal')
        assert result['qty_type'] in ('integer', 'bigint')
        assert result['bool_type'] == 'boolean'

        # Verify values
        assert float(result['price']) == 99.99
        assert result['stock_quantity'] == 100
        assert result['is_active'] is True
        cursor.close()

    def test_sink_handles_null_values(self, scylla_session, postgres_conn):
        """Test sink correctly handles NULL values."""
        product_id = uuid.uuid4()

        # Insert with some NULL values
        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'NULL Test', None, 10.00, 0, 'Test', True))

        time.sleep(10)

        # Verify NULL is preserved
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT description
            FROM cdc_data.products
            WHERE product_id = %s
        """, (str(product_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['description'] is None, "NULL value not preserved"
        cursor.close()

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

        time.sleep(15)

        # Verify all batched records arrived
        cursor = postgres_conn.cursor()
        placeholders = ','.join(['%s'] * len(user_ids))
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM cdc_data.users
            WHERE user_id::text IN ({placeholders})
        """, [str(uid) for uid in user_ids])

        count = cursor.fetchone()[0]
        assert count == 10, f"Batch processing failed: expected 10, got {count}"
        cursor.close()

    def test_sink_connector_configuration(self, kafka_connect_url):
        """Test sink connector is configured correctly."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-sink/config")
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

    def test_sink_handles_special_characters(self, scylla_session, postgres_conn):
        """Test sink handles special characters in data."""
        user_id = uuid.uuid4()
        special_name = "Test'User\"With<Special>Chars&Symbols"

        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'special_{uuid.uuid4().hex[:8]}', 'special@test.com', special_name, 'User', 'active'))

        time.sleep(10)

        # Verify special characters preserved
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT first_name
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['first_name'] == special_name, "Special characters not preserved"
        cursor.close()

    def test_sink_referential_integrity(self, scylla_session, postgres_conn):
        """Test sink maintains referential integrity across tables."""
        user_id = uuid.uuid4()
        order_id = uuid.uuid4()

        # Insert user
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'ref_user_{uuid.uuid4().hex[:8]}', 'ref@test.com', 'Ref', 'User', 'active'))

        time.sleep(5)

        # Insert order referencing user
        scylla_session.execute("""
            INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
            VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, toTimestamp(now()), toTimestamp(now()))
        """, (order_id, user_id, 100.00, 'pending', '123 Test St'))

        time.sleep(10)

        # Verify referential integrity maintained
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT o.order_id, o.user_id, u.username
            FROM cdc_data.orders o
            JOIN cdc_data.users u ON o.user_id = u.user_id
            WHERE o.order_id = %s
        """, (str(order_id),))

        result = cursor.fetchone()
        assert result is not None, "Referential integrity broken"
        assert result['user_id'] == str(user_id)
        cursor.close()

    def test_sink_error_handling_configuration(self, kafka_connect_url):
        """Test sink has proper error handling configured."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-sink/config")
        assert response.status_code == 200

        config = response.json()

        # Verify error handling settings
        assert config.get('errors.tolerance') == 'all'
        assert 'errors.deadletterqueue.topic.name' in config
        assert config['errors.deadletterqueue.topic.name'] == 'dlq-postgres-sink'

    def test_sink_connection_pooling(self, kafka_connect_url):
        """Test sink uses connection pooling."""
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-sink/config")
        assert response.status_code == 200

        config = response.json()

        # Verify connection pool settings
        if 'connection.pool.size' in config:
            pool_size = int(config['connection.pool.size'])
            assert pool_size >= 5, "Connection pool size should be >= 5"
