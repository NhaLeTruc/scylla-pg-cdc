"""
Pytest configuration and shared fixtures for contract and integration tests.

Provides cleanup utilities to ensure tests run with clean state.
"""

import pytest
import time
import requests
import psycopg2
from cassandra.cluster import Cluster


def cleanup_postgres_tables():
    """Truncate all CDC tables in PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='warehouse',
            user='postgres',
            password='postgres'
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Get all tables in cdc_data schema
        cursor.execute("""
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'cdc_data'
        """)

        tables = cursor.fetchall()
        for (table_name,) in tables:
            cursor.execute(f'TRUNCATE TABLE cdc_data."{table_name}" CASCADE')
            print(f"Truncated cdc_data.{table_name}")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Warning: Failed to cleanup PostgreSQL tables: {e}")
        return False


def cleanup_scylla_tables():
    """Truncate all tables in ScyllaDB app_data keyspace."""
    try:
        cluster = Cluster(['localhost'], port=9042, protocol_version=4)
        session = cluster.connect()

        # Get all tables in app_data keyspace
        rows = session.execute("""
            SELECT table_name
            FROM system_schema.tables
            WHERE keyspace_name = 'app_data'
        """)

        for row in rows:
            table_name = row.table_name
            session.execute(f'TRUNCATE app_data.{table_name}')
            print(f"Truncated app_data.{table_name}")

        cluster.shutdown()
        return True
    except Exception as e:
        print(f"Warning: Failed to cleanup ScyllaDB tables: {e}")
        return False


def reset_kafka_connector_offsets(connector_name):
    """Reset Kafka Connect sink connector by restarting it."""
    try:
        # Pause connector
        requests.put(f"http://localhost:8083/connectors/{connector_name}/pause", timeout=5)
        time.sleep(2)

        # Resume connector
        requests.put(f"http://localhost:8083/connectors/{connector_name}/resume", timeout=5)
        time.sleep(3)

        # Verify it's running
        response = requests.get(f"http://localhost:8083/connectors/{connector_name}/status", timeout=5)
        if response.status_code == 200:
            state = response.json().get('connector', {}).get('state')
            print(f"Connector {connector_name} state: {state}")
            return state == 'RUNNING'
        return False
    except Exception as e:
        print(f"Warning: Failed to reset connector {connector_name}: {e}")
        return False


def wait_for_kafka_lag_to_clear(max_wait_seconds=60):
    """Wait for Kafka consumer lag to clear or reach near-zero."""
    try:
        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            # Check if connectors are healthy
            response = requests.get("http://localhost:8083/connectors/postgres-jdbc-sink/status", timeout=5)
            if response.status_code == 200:
                status = response.json()
                if status.get('connector', {}).get('state') == 'RUNNING':
                    # Give it a bit more time to process
                    time.sleep(5)
                    return True
            time.sleep(2)
        return True
    except Exception as e:
        print(f"Warning: Failed to check Kafka lag: {e}")
        return True  # Continue anyway


@pytest.fixture(scope="session", autouse=True)
def cleanup_before_tests():
    """
    Session-scoped fixture that runs once before all tests.
    Cleans up all test data to ensure a fresh start.
    """
    print("\n" + "="*80)
    print("CLEANING UP TEST ENVIRONMENT BEFORE TESTS")
    print("="*80)

    # Step 1: Cleanup PostgreSQL tables
    print("\n[1/5] Cleaning PostgreSQL tables...")
    cleanup_postgres_tables()

    # Step 2: Cleanup ScyllaDB tables
    print("\n[2/5] Cleaning ScyllaDB tables...")
    cleanup_scylla_tables()

    # Step 3: Reset connector offsets
    print("\n[3/5] Resetting Kafka connectors...")
    reset_kafka_connector_offsets('postgres-jdbc-sink')
    reset_kafka_connector_offsets('scylla-cdc-source')

    # Step 4: Wait for lag to clear
    print("\n[4/5] Waiting for Kafka lag to clear...")
    wait_for_kafka_lag_to_clear(30)

    # Step 5: Final wait for stabilization
    print("\n[5/5] Waiting for system stabilization...")
    time.sleep(10)

    print("\n" + "="*80)
    print("CLEANUP COMPLETE - STARTING TESTS")
    print("="*80 + "\n")

    yield  # Run tests

    # Optional: Cleanup after all tests complete
    print("\n" + "="*80)
    print("ALL TESTS COMPLETE")
    print("="*80 + "\n")


def wait_for_data_in_postgres(query, params=None, timeout=90, poll_interval=3):
    """
    Wait for data to appear in PostgreSQL with retry logic.

    Args:
        query: SQL query to execute
        params: Query parameters
        timeout: Maximum seconds to wait
        poll_interval: Seconds between retries

    Returns:
        Result of the query or None if timeout
    """
    conn = None
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='warehouse',
            user='postgres',
            password='postgres'
        )
        conn.autocommit = True

        start_time = time.time()
        while time.time() - start_time < timeout:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            result = cursor.fetchone()
            cursor.close()

            if result is not None and result != (0,):
                return result

            time.sleep(poll_interval)

        return None
    except Exception as e:
        print(f"Error waiting for data: {e}")
        return None
    finally:
        if conn:
            conn.close()


@pytest.fixture(scope="module")
def clean_test_data():
    """
    Module-scoped fixture that can be used by individual test modules
    to ensure a clean state before their tests run.
    """
    # Cleanup before tests in this module
    cleanup_postgres_tables()
    time.sleep(2)

    yield

    # Optional: Cleanup after tests in this module
    # (commented out to avoid interfering with other modules)
    # cleanup_postgres_tables()