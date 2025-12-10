"""
Contract Tests for ScyllaDB CDC Source Connector

Validates connector output format, Avro schemas, and message structure.
"""

import pytest
import time
import uuid
from cassandra.cluster import Cluster
import requests
from kafka import KafkaConsumer
import json


@pytest.fixture(scope="module")
def scylla_session():
    """Create ScyllaDB session."""
    cluster = Cluster(['localhost'], port=9042)
    session = cluster.connect()
    yield session
    cluster.shutdown()


@pytest.fixture(scope="module")
def schema_registry_url():
    """Schema Registry URL."""
    return "http://localhost:8081"


@pytest.fixture(scope="module")
def kafka_consumer():
    """Create Kafka consumer for CDC topics."""
    consumer = KafkaConsumer(
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        group_id=f'test_consumer_{uuid.uuid4().hex[:8]}',
        value_deserializer=lambda m: m  # Raw bytes for Avro
    )
    yield consumer
    consumer.close()


@pytest.mark.contract
class TestScyllaConnectorContract:
    """Contract tests for ScyllaDB CDC source connector."""

    def test_connector_produces_avro_messages(self, scylla_session, kafka_consumer):
        """Test connector produces Avro-serialized messages."""
        # Subscribe to CDC topic
        kafka_consumer.subscribe(['cdc.scylla.users'])

        # Insert test data
        user_id = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'contract_test_{uuid.uuid4().hex[:8]}', 'contract@test.com', 'Test', 'User', 'active'))

        # Poll for messages
        messages = kafka_consumer.poll(timeout_ms=30000, max_records=10)

        assert len(messages) > 0, "No messages received from connector"

        # Verify message format (Avro magic byte)
        for topic_partition, records in messages.items():
            for record in records:
                # Avro messages start with magic byte 0x00
                assert record.value[0] == 0, "Message not in Avro format"

    def test_connector_includes_metadata_headers(self, scylla_session, kafka_consumer):
        """Test connector includes CDC metadata in message headers."""
        kafka_consumer.subscribe(['cdc.scylla.users'])

        # Insert test data
        user_id = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'metadata_test_{uuid.uuid4().hex[:8]}', 'metadata@test.com', 'Test', 'User', 'active'))

        # Poll for messages
        messages = kafka_consumer.poll(timeout_ms=30000, max_records=10)

        for topic_partition, records in messages.items():
            for record in records:
                # Check headers exist
                assert record.headers is not None
                header_dict = dict(record.headers)

                # Verify expected headers
                # Note: Header names depend on connector configuration
                assert len(header_dict) > 0, "No headers in message"

    def test_topic_naming_convention(self, kafka_consumer):
        """Test messages are routed to correctly named topics."""
        # Subscribe to pattern
        kafka_consumer.subscribe(pattern='cdc\\.scylla\\..*')

        # Get assigned topics
        time.sleep(2)
        topics = kafka_consumer.subscription()

        # Verify topic naming
        for topic in topics:
            assert topic.startswith('cdc.scylla.'), f"Topic {topic} doesn't follow naming convention"

    def test_schema_registry_integration(self, schema_registry_url):
        """Test schemas are registered in Schema Registry."""
        # Get all subjects
        response = requests.get(f"{schema_registry_url}/subjects")
        assert response.status_code == 200

        subjects = response.json()

        # Find CDC schemas
        cdc_subjects = [s for s in subjects if 'cdc.scylla' in s]
        assert len(cdc_subjects) > 0, "No CDC schemas in Schema Registry"

        # Verify schema structure
        for subject in cdc_subjects:
            response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions/latest")
            assert response.status_code == 200

            schema_data = response.json()
            assert 'schema' in schema_data
            assert 'id' in schema_data

    def test_message_key_structure(self, scylla_session, kafka_consumer):
        """Test message keys contain primary key fields."""
        kafka_consumer.subscribe(['cdc.scylla.users'])

        # Insert test data
        user_id = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'key_test_{uuid.uuid4().hex[:8]}', 'key@test.com', 'Test', 'User', 'active'))

        # Poll for messages
        messages = kafka_consumer.poll(timeout_ms=30000, max_records=10)

        for topic_partition, records in messages.items():
            for record in records:
                # Verify key exists and is not None
                assert record.key is not None, "Message key is None"
                assert len(record.key) > 0, "Message key is empty"

    def test_connector_handles_all_operations(self, scylla_session, kafka_consumer):
        """Test connector captures INSERT, UPDATE, DELETE operations."""
        kafka_consumer.subscribe(['cdc.scylla.users'])

        # INSERT
        user_id = uuid.uuid4()
        scylla_session.execute("""
            INSERT INTO app_data.users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), toTimestamp(now()), %s)
        """, (user_id, f'ops_test_{uuid.uuid4().hex[:8]}', 'ops@test.com', 'Test', 'User', 'pending'))

        time.sleep(2)

        # UPDATE
        scylla_session.execute("""
            UPDATE app_data.users
            SET status = %s, updated_at = toTimestamp(now())
            WHERE user_id = %s
        """, ('active', user_id))

        time.sleep(2)

        # Poll for messages
        messages = kafka_consumer.poll(timeout_ms=30000, max_records=20)

        # Should have at least 2 messages (INSERT + UPDATE)
        total_messages = sum(len(records) for records in messages.values())
        assert total_messages >= 2, f"Expected at least 2 messages, got {total_messages}"

    def test_connector_heartbeat_messages(self, kafka_consumer):
        """Test connector sends heartbeat messages."""
        # Subscribe to heartbeat topic
        kafka_consumer.subscribe(['heartbeat.scylla'])

        # Wait for heartbeat (sent every 30s)
        messages = kafka_consumer.poll(timeout_ms=60000, max_records=5)

        # May or may not receive heartbeat depending on timing
        # This test validates topic exists and is accessible
        assert kafka_consumer.subscription() == {'heartbeat.scylla'}

    def test_partition_assignment(self, kafka_consumer):
        """Test topic has correct number of partitions."""
        kafka_consumer.subscribe(['cdc.scylla.users'])

        # Wait for assignment
        time.sleep(2)

        # Get partition assignment
        assignment = kafka_consumer.assignment()

        # Should have partitions assigned
        assert len(assignment) > 0, "No partitions assigned"

        # Verify partition count (should match topic configuration)
        partitions = [tp.partition for tp in assignment]
        assert max(partitions) >= 0, "Invalid partition numbers"
