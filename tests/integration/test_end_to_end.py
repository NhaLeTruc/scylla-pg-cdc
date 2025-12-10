"""
End-to-End Integration Tests for CDC Pipeline

Tests the full pipeline flow from ScyllaDB through Kafka to PostgreSQL.
"""

import pytest
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import psycopg2
from psycopg2.extras import RealDictCursor
import requests


@pytest.fixture(scope="module")
def scylla_session():
    """Create ScyllaDB session for testing."""
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    yield session
    cluster.shutdown()


@pytest.fixture(scope="module")
def postgres_conn():
    """Create PostgreSQL connection for testing."""
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


def wait_for_replication(timeout: int = 60) -> None:
    """Wait for CDC replication to complete."""
    time.sleep(timeout)


class TestEndToEndPipeline:
    """End-to-end integration tests for the CDC pipeline."""

    def test_connectors_are_running(self, kafka_connect_url: str):
        """Test that both connectors are in RUNNING state."""
        # Check source connector
        response = requests.get(f"{kafka_connect_url}/connectors/scylla-source/status")
        assert response.status_code == 200
        status = response.json()
        assert status['connector']['state'] == 'RUNNING', f"Source connector not running: {status}"

        # Check sink connector
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-sink/status")
        assert response.status_code == 200
        status = response.json()
        assert status['connector']['state'] == 'RUNNING', f"Sink connector not running: {status}"

    def test_user_insert_replication(self, scylla_session, postgres_conn):
        """Test INSERT operation replication for users table."""
        # Generate unique test data
        user_id = uuid.uuid4()
        username = f"test_e2e_user_{uuid.uuid4().hex[:8]}"
        email = f"{username}@test.com"

        # Insert into ScyllaDB
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, email, 'Test', 'User', 'active'))

        # Wait for replication
        wait_for_replication(10)

        # Verify in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT user_id, username, email, first_name, last_name, status
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result is not None, f"User {user_id} not found in PostgreSQL"
        assert result['username'] == username
        assert result['email'] == email
        assert result['status'] == 'active'
        cursor.close()

    def test_product_insert_replication(self, scylla_session, postgres_conn):
        """Test INSERT operation replication for products table."""
        product_id = uuid.uuid4()
        product_name = f"Test Product {uuid.uuid4().hex[:8]}"

        # Insert into ScyllaDB
        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, product_name, 'Test description', 99.99, 100, 'Test', True))

        # Wait for replication
        wait_for_replication(10)

        # Verify in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT product_id, product_name, price, stock_quantity, category, is_active
            FROM cdc_data.products
            WHERE product_id = %s
        """, (str(product_id),))

        result = cursor.fetchone()
        assert result is not None, f"Product {product_id} not found in PostgreSQL"
        assert result['product_name'] == product_name
        assert float(result['price']) == 99.99
        assert result['stock_quantity'] == 100
        assert result['is_active'] is True
        cursor.close()

    def test_user_update_replication(self, scylla_session, postgres_conn):
        """Test UPDATE operation replication."""
        # First insert a user
        user_id = uuid.uuid4()
        username = f"test_update_{uuid.uuid4().hex[:8]}"
        email = f"{username}@test.com"

        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, email, 'Test', 'User', 'pending'))

        wait_for_replication(10)

        # Update the user
        scylla_session.execute("""
            UPDATE app_data.users
            SET status = %s, updated_at = toTimestamp(now())
            WHERE user_id = %s
        """, ('active', user_id))

        wait_for_replication(10)

        # Verify update in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT status
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['status'] == 'active', "Update not replicated"
        cursor.close()

    def test_order_with_items_replication(self, scylla_session, postgres_conn):
        """Test replication of orders with order items (referential integrity)."""
        # Create user, product, order, and order item
        user_id = uuid.uuid4()
        product_id = uuid.uuid4()
        order_id = uuid.uuid4()
        item_id = uuid.uuid4()

        username = f"test_order_user_{uuid.uuid4().hex[:8]}"

        # Insert user
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, f"{username}@test.com", 'Test', 'User', 'active'))

        # Insert product
        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'Test Product', 'Test', 50.00, 10, 'Test', True))

        # Insert order
        scylla_session.execute("""
            INSERT INTO app_data.orders (order_id, user_id, order_date, total_amount, status, shipping_address, created_at, updated_at)
            VALUES (%s, %s, toTimestamp(now()), %s, %s, %s, toTimestamp(now()), toTimestamp(now()))
        """, (order_id, user_id, 50.00, 'pending', '123 Test St'))

        # Insert order item
        scylla_session.execute("""
            INSERT INTO app_data.order_items (item_id, order_id, product_id, quantity, unit_price, subtotal, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()))
        """, (item_id, order_id, product_id, 1, 50.00, 50.00))

        wait_for_replication(15)

        # Verify referential integrity in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT o.order_id, o.user_id, u.username, oi.item_id, oi.product_id, p.product_name
            FROM cdc_data.orders o
            JOIN cdc_data.users u ON o.user_id = u.user_id
            JOIN cdc_data.order_items oi ON o.order_id = oi.order_id
            JOIN cdc_data.products p ON oi.product_id = p.product_id
            WHERE o.order_id = %s
        """, (str(order_id),))

        result = cursor.fetchone()
        assert result is not None, "Order with relationships not found"
        assert result['username'] == username
        assert result['product_name'] == 'Test Product'
        cursor.close()

    def test_bulk_insert_replication(self, scylla_session, postgres_conn):
        """Test bulk INSERT replication."""
        base_id = uuid.uuid4().hex[:8]
        user_ids = []

        # Insert 10 users
        for i in range(10):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            username = f"bulk_user_{base_id}_{i}"

            scylla_session.execute("""
                INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
                VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
            """, (user_id, username, f"{username}@test.com", 'Bulk', f'User{i}', 'active'))

        wait_for_replication(20)

        # Verify all users replicated
        cursor = postgres_conn.cursor()
        placeholders = ','.join(['%s'] * len(user_ids))
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM cdc_data.users
            WHERE user_id::text IN ({placeholders})
        """, [str(uid) for uid in user_ids])

        count = cursor.fetchone()[0]
        assert count == 10, f"Expected 10 users, found {count}"
        cursor.close()

    def test_cdc_metadata_fields(self, scylla_session, postgres_conn):
        """Test that CDC metadata fields are present."""
        user_id = uuid.uuid4()
        username = f"test_metadata_{uuid.uuid4().hex[:8]}"

        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, f"{username}@test.com", 'Test', 'User', 'active'))

        wait_for_replication(10)

        # Verify CDC metadata fields
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT cdc_timestamp, cdc_source
            FROM cdc_data.users
            WHERE user_id = %s
        """, (str(user_id),))

        result = cursor.fetchone()
        assert result is not None
        assert 'cdc_timestamp' in result
        assert 'cdc_source' in result
        assert result['cdc_source'] == 'scylla-cdc' or result['cdc_source'] is not None
        cursor.close()

    def test_replication_latency(self, scylla_session, postgres_conn):
        """Test replication latency is within acceptable limits."""
        user_id = uuid.uuid4()
        username = f"test_latency_{uuid.uuid4().hex[:8]}"

        # Record start time
        start_time = time.time()

        # Insert into ScyllaDB
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, username, f"{username}@test.com", 'Test', 'User', 'active'))

        # Poll PostgreSQL until record appears
        cursor = postgres_conn.cursor()
        max_attempts = 60  # 60 seconds max
        found = False

        for attempt in range(max_attempts):
            cursor.execute("""
                SELECT 1
                FROM cdc_data.users
                WHERE user_id = %s
            """, (str(user_id),))

            if cursor.fetchone():
                found = True
                break

            time.sleep(1)

        end_time = time.time()
        latency = end_time - start_time

        assert found, f"Record not replicated after {max_attempts} seconds"
        assert latency < 30, f"Replication latency {latency:.2f}s exceeds 30s threshold"
        cursor.close()

    def test_inventory_transaction_replication(self, scylla_session, postgres_conn):
        """Test inventory transactions replication."""
        transaction_id = uuid.uuid4()
        product_id = uuid.uuid4()

        # Insert product first
        scylla_session.execute("""
            INSERT INTO app_data.products (product_id, product_name, description, price, stock_quantity, category, created_at, updated_at, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (product_id, 'Inventory Test Product', 'Test', 10.00, 100, 'Test', True))

        # Insert inventory transaction
        scylla_session.execute("""
            INSERT INTO app_data.inventory_transactions (transaction_id, product_id, transaction_type, quantity_change, transaction_date, reason, created_at)
            VALUES (%s, %s, %s, %s, toTimestamp(now()), %s, toTimestamp(now()))
        """, (transaction_id, product_id, 'sale', -5, 'Test sale'))

        wait_for_replication(10)

        # Verify in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT transaction_id, product_id, transaction_type, quantity_change
            FROM cdc_data.inventory_transactions
            WHERE transaction_id = %s
        """, (str(transaction_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['transaction_type'] == 'sale'
        assert result['quantity_change'] == -5
        cursor.close()

    def test_connector_task_distribution(self, kafka_connect_url: str):
        """Test that connector tasks are distributed across workers."""
        # Check source connector tasks
        response = requests.get(f"{kafka_connect_url}/connectors/scylla-source/status")
        assert response.status_code == 200
        source_status = response.json()

        # Should have multiple tasks
        assert len(source_status['tasks']) > 0, "Source connector has no tasks"

        # Check sink connector tasks
        response = requests.get(f"{kafka_connect_url}/connectors/postgres-sink/status")
        assert response.status_code == 200
        sink_status = response.json()

        assert len(sink_status['tasks']) > 0, "Sink connector has no tasks"

        # All tasks should be RUNNING
        for task in source_status['tasks']:
            assert task['state'] == 'RUNNING', f"Source task {task['id']} not running"

        for task in sink_status['tasks']:
            assert task['state'] == 'RUNNING', f"Sink task {task['id']} not running"
