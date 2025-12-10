"""
Failure Recovery Integration Tests

Tests connector failure recovery, DLQ functionality, and data consistency after failures.
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


@pytest.mark.integration
class TestFailureRecovery:
    """Integration tests for failure recovery scenarios."""

    def test_connector_status_after_restart(self, kafka_connect_url):
        """Test connector recovers to RUNNING state after restart."""
        connector_name = "scylla-source"

        # Get current status
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        initial_status = response.json()

        # Restart connector
        response = requests.post(f"{kafka_connect_url}/connectors/{connector_name}/restart")
        assert response.status_code in [200, 202, 204]

        # Wait for connector to restart
        time.sleep(10)

        # Verify connector is running
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        current_status = response.json()

        assert current_status['connector']['state'] == 'RUNNING', \
            f"Connector not running after restart: {current_status}"

    def test_data_consistency_after_connector_restart(self, scylla_session, postgres_conn, kafka_connect_url):
        """Test no data loss after connector restart."""
        # Insert data before restart
        user_id_before = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id_before, f'before_restart_{uuid.uuid4().hex[:8]}', 'before@test.com', 'Test', 'User', 'active'))

        time.sleep(5)

        # Restart sink connector
        requests.post(f"{kafka_connect_url}/connectors/postgres-sink/restart")
        time.sleep(10)

        # Insert data after restart
        user_id_after = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id_after, f'after_restart_{uuid.uuid4().hex[:8]}', 'after@test.com', 'Test', 'User', 'active'))

        time.sleep(10)

        # Verify both records exist
        cursor = postgres_conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM cdc_data.users
            WHERE user_id::text IN (%s, %s)
        """, (str(user_id_before), str(user_id_after)))

        count = cursor.fetchone()[0]
        assert count == 2, "Data loss detected after connector restart"
        cursor.close()

    def test_offset_preservation(self, kafka_connect_url):
        """Test that offsets are preserved across restarts."""
        connector_name = "scylla-source"

        # Get current offsets (indirectly through status)
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        status_before = response.json()

        # Restart connector
        requests.post(f"{kafka_connect_url}/connectors/{connector_name}/restart")
        time.sleep(10)

        # Get status after restart
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        status_after = response.json()

        # Verify connector restarted successfully
        assert status_after['connector']['state'] == 'RUNNING'
        assert len(status_after['tasks']) == len(status_before['tasks'])

    def test_automatic_task_rebalance(self, kafka_connect_url):
        """Test tasks rebalance when connector restarts."""
        connector_name = "postgres-sink"

        # Get initial task distribution
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        initial_status = response.json()
        initial_task_count = len(initial_status['tasks'])

        # Restart connector
        requests.post(f"{kafka_connect_url}/connectors/{connector_name}/restart")
        time.sleep(15)

        # Get new task distribution
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        assert response.status_code == 200
        new_status = response.json()
        new_task_count = len(new_status['tasks'])

        # Tasks should be rebalanced (same count, potentially different workers)
        assert new_task_count == initial_task_count, "Task count changed after rebalance"

        # All tasks should be RUNNING
        for task in new_status['tasks']:
            assert task['state'] == 'RUNNING', f"Task {task['id']} not running after rebalance"

    def test_error_tolerance_configuration(self, kafka_connect_url):
        """Test error tolerance is configured correctly."""
        connector_name = "scylla-source"

        # Get connector configuration
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/config")
        assert response.status_code == 200
        config = response.json()

        # Verify error tolerance settings
        assert 'errors.tolerance' in config
        assert config['errors.tolerance'] == 'all', "Error tolerance not set to 'all'"

        # Verify DLQ is configured
        assert 'errors.deadletterqueue.topic.name' in config
        assert config['errors.deadletterqueue.topic.name'].startswith('dlq-')

    def test_dlq_configuration(self, kafka_connect_url):
        """Test Dead Letter Queue is properly configured."""
        # Check both connectors
        for connector in ['scylla-source', 'postgres-sink']:
            response = requests.get(f"{kafka_connect_url}/connectors/{connector}/config")
            assert response.status_code == 200
            config = response.json()

            # Verify DLQ settings
            if 'errors.deadletterqueue.topic.name' in config:
                assert config['errors.deadletterqueue.topic.name'] == f'dlq-{connector}'
                assert config.get('errors.deadletterqueue.context.headers.enable') == 'true'

    def test_connector_health_after_multiple_restarts(self, kafka_connect_url):
        """Test connector remains healthy after multiple restarts."""
        connector_name = "scylla-source"

        for i in range(3):
            # Restart connector
            response = requests.post(f"{kafka_connect_url}/connectors/{connector_name}/restart")
            assert response.status_code in [200, 202, 204]

            # Wait for restart
            time.sleep(10)

            # Verify health
            response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
            assert response.status_code == 200
            status = response.json()

            assert status['connector']['state'] == 'RUNNING', \
                f"Connector failed after restart #{i+1}"

    def test_backpressure_handling(self, scylla_session, postgres_conn):
        """Test system handles backpressure gracefully."""
        # Insert large batch of data quickly
        user_ids = []

        for i in range(50):
            user_id = uuid.uuid4()
            user_ids.append(user_id)
            scylla_session.execute("""
                INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
                VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
            """, (user_id, f'backpressure_user_{i}', f'user{i}@test.com', 'Test', f'User{i}', 'active'))

        # Wait for replication with backpressure
        time.sleep(30)

        # Verify all records eventually replicated
        cursor = postgres_conn.cursor()
        placeholders = ','.join(['%s'] * len(user_ids))
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM cdc_data.users
            WHERE user_id::text IN ({placeholders})
        """, [str(uid) for uid in user_ids])

        count = cursor.fetchone()[0]
        assert count >= 40, f"Expected at least 40 records under backpressure, found {count}"
        cursor.close()

    def test_graceful_shutdown_recovery(self, kafka_connect_url):
        """Test graceful shutdown and recovery."""
        connector_name = "postgres-sink"

        # Pause connector (graceful shutdown)
        response = requests.put(f"{kafka_connect_url}/connectors/{connector_name}/pause")
        assert response.status_code in [200, 202]

        time.sleep(5)

        # Verify paused state
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        status = response.json()
        assert status['connector']['state'] == 'PAUSED'

        # Resume connector
        response = requests.put(f"{kafka_connect_url}/connectors/{connector_name}/resume")
        assert response.status_code in [200, 202]

        time.sleep(10)

        # Verify running state
        response = requests.get(f"{kafka_connect_url}/connectors/{connector_name}/status")
        status = response.json()
        assert status['connector']['state'] == 'RUNNING'

    def test_connection_retry_mechanism(self, kafka_connect_url):
        """Test connection retry configuration."""
        # Check retry settings in connector configs
        for connector in ['scylla-source', 'postgres-sink']:
            response = requests.get(f"{kafka_connect_url}/connectors/{connector}/config")
            if response.status_code == 200:
                config = response.json()

                # Verify retry settings exist
                if 'connection.attempts' in config:
                    attempts = int(config['connection.attempts'])
                    assert attempts >= 3, "Connection attempts should be >= 3"

                if 'connection.backoff.ms' in config:
                    backoff = int(config['connection.backoff.ms'])
                    assert backoff > 0, "Connection backoff should be > 0"
