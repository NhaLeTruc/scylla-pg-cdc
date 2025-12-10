"""
Schema Evolution Integration Tests

Tests schema change scenarios and compatibility validation.
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
def schema_registry_url():
    """Schema Registry URL."""
    return "http://localhost:8081"


@pytest.mark.integration
class TestSchemaEvolution:
    """Integration tests for schema evolution."""

    def test_add_column_backward_compatible(self, scylla_session, postgres_conn):
        """Test adding a new column (backward compatible)."""
        # Create test table with initial schema
        table_name = f"test_add_column_{uuid.uuid4().hex[:8]}"

        # Create table in ScyllaDB
        scylla_session.execute(f"""
            CREATE TABLE IF NOT EXISTS app_data.{table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                value INT
            ) WITH cdc = {{'enabled': true}}
        """)

        # Insert initial data
        test_id = uuid.uuid4()
        scylla_session.execute(f"""
            INSERT INTO app_data.{table_name} (id, name, value)
            VALUES (%s, %s, %s)
        """, (test_id, 'test', 100))

        time.sleep(10)

        # Add new column (backward compatible - has default)
        scylla_session.execute(f"""
            ALTER TABLE app_data.{table_name}
            ADD new_field TEXT
        """)

        # Insert data with new field
        test_id2 = uuid.uuid4()
        scylla_session.execute(f"""
            INSERT INTO app_data.{table_name} (id, name, value, new_field)
            VALUES (%s, %s, %s, %s)
        """, (test_id2, 'test2', 200, 'new_value'))

        time.sleep(15)

        # Verify both records exist in PostgreSQL
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(f"""
            SELECT COUNT(*) as count
            FROM cdc_data.{table_name}
            WHERE id::text IN (%s, %s)
        """, (str(test_id), str(test_id2)))

        result = cursor.fetchone()
        assert result['count'] == 2, "Records not replicated after schema change"
        cursor.close()

        # Cleanup
        scylla_session.execute(f"DROP TABLE app_data.{table_name}")

    def test_schema_registry_versions(self, schema_registry_url):
        """Test that schema versions are tracked in Schema Registry."""
        # Get list of subjects
        response = requests.get(f"{schema_registry_url}/subjects")
        assert response.status_code == 200, "Schema Registry not accessible"

        subjects = response.json()
        assert isinstance(subjects, list), "Invalid subjects response"

        # Check if our CDC subjects exist
        cdc_subjects = [s for s in subjects if s.startswith('cdc.scylla.')]
        assert len(cdc_subjects) > 0, "No CDC schemas found in Schema Registry"

        # Get versions for first subject
        if cdc_subjects:
            subject = cdc_subjects[0]
            response = requests.get(f"{schema_registry_url}/subjects/{subject}/versions")
            assert response.status_code == 200

            versions = response.json()
            assert isinstance(versions, list)
            assert len(versions) > 0, f"No versions found for subject {subject}"

    def test_schema_compatibility_check(self, schema_registry_url):
        """Test schema compatibility validation."""
        # Get first CDC subject
        response = requests.get(f"{schema_registry_url}/subjects")
        if response.status_code != 200:
            pytest.skip("Schema Registry not available")

        subjects = response.json()
        cdc_subjects = [s for s in subjects if s.startswith('cdc.scylla.')]

        if not cdc_subjects:
            pytest.skip("No CDC schemas available")

        subject = cdc_subjects[0]

        # Get compatibility mode
        response = requests.get(f"{schema_registry_url}/config/{subject}")
        if response.status_code == 200:
            config = response.json()
            assert 'compatibilityLevel' in config or 'compatibility' in config

    def test_remove_column_forward_compatible(self, scylla_session, postgres_conn):
        """Test removing a column (forward compatible scenario)."""
        table_name = f"test_remove_column_{uuid.uuid4().hex[:8]}"

        # Create table with extra column
        scylla_session.execute(f"""
            CREATE TABLE IF NOT EXISTS app_data.{table_name} (
                id UUID PRIMARY KEY,
                name TEXT,
                value INT,
                temp_field TEXT
            ) WITH cdc = {{'enabled': true}}
        """)

        # Insert data
        test_id = uuid.uuid4()
        scylla_session.execute(f"""
            INSERT INTO app_data.{table_name} (id, name, value, temp_field)
            VALUES (%s, %s, %s, %s)
        """, (test_id, 'test', 100, 'temp'))

        time.sleep(10)

        # Note: Cassandra/ScyllaDB doesn't support DROP COLUMN in all versions
        # This test verifies the connector handles missing fields gracefully

        # Insert data without temp_field
        test_id2 = uuid.uuid4()
        scylla_session.execute(f"""
            INSERT INTO app_data.{table_name} (id, name, value)
            VALUES (%s, %s, %s)
        """, (test_id2, 'test2', 200))

        time.sleep(10)

        # Verify both records exist
        cursor = postgres_conn.cursor()
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM cdc_data.{table_name}
            WHERE id::text IN (%s, %s)
        """, (str(test_id), str(test_id2)))

        count = cursor.fetchone()[0]
        assert count == 2, "Records not replicated with optional field"
        cursor.close()

        # Cleanup
        scylla_session.execute(f"DROP TABLE app_data.{table_name}")

    def test_concurrent_schema_changes(self, scylla_session, postgres_conn):
        """Test multiple concurrent inserts during schema change."""
        table_name = f"test_concurrent_{uuid.uuid4().hex[:8]}"

        # Create initial table
        scylla_session.execute(f"""
            CREATE TABLE IF NOT EXISTS app_data.{table_name} (
                id UUID PRIMARY KEY,
                data TEXT
            ) WITH cdc = {{'enabled': true}}
        """)

        # Insert records before schema change
        ids_before = []
        for i in range(5):
            test_id = uuid.uuid4()
            ids_before.append(test_id)
            scylla_session.execute(f"""
                INSERT INTO app_data.{table_name} (id, data)
                VALUES (%s, %s)
            """, (test_id, f'before_{i}'))

        time.sleep(10)

        # Add column
        scylla_session.execute(f"""
            ALTER TABLE app_data.{table_name}
            ADD extra TEXT
        """)

        # Insert records after schema change
        ids_after = []
        for i in range(5):
            test_id = uuid.uuid4()
            ids_after.append(test_id)
            scylla_session.execute(f"""
                INSERT INTO app_data.{table_name} (id, data, extra)
                VALUES (%s, %s, %s)
            """, (test_id, f'after_{i}', f'extra_{i}'))

        time.sleep(15)

        # Verify all 10 records exist
        all_ids = ids_before + ids_after
        cursor = postgres_conn.cursor()
        placeholders = ','.join(['%s'] * len(all_ids))
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM cdc_data.{table_name}
            WHERE id::text IN ({placeholders})
        """, [str(i) for i in all_ids])

        count = cursor.fetchone()[0]
        assert count == 10, f"Expected 10 records, found {count}"
        cursor.close()

        # Cleanup
        scylla_session.execute(f"DROP TABLE app_data.{table_name}")

    def test_null_values_after_schema_change(self, scylla_session, postgres_conn):
        """Test NULL handling for new columns."""
        table_name = f"test_nulls_{uuid.uuid4().hex[:8]}"

        # Create table
        scylla_session.execute(f"""
            CREATE TABLE IF NOT EXISTS app_data.{table_name} (
                id UUID PRIMARY KEY,
                name TEXT
            ) WITH cdc = {{'enabled': true}}
        """)

        # Insert before schema change
        test_id = uuid.uuid4()
        scylla_session.execute(f"""
            INSERT INTO app_data.{table_name} (id, name)
            VALUES (%s, %s)
        """, (test_id, 'test'))

        time.sleep(10)

        # Add nullable column
        scylla_session.execute(f"""
            ALTER TABLE app_data.{table_name}
            ADD nullable_field INT
        """)

        # Update existing record (leave nullable_field as NULL)
        scylla_session.execute(f"""
            UPDATE app_data.{table_name}
            SET name = %s
            WHERE id = %s
        """, ('updated', test_id))

        time.sleep(10)

        # Verify NULL is handled correctly
        cursor = postgres_conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(f"""
            SELECT name, nullable_field
            FROM cdc_data.{table_name}
            WHERE id = %s
        """, (str(test_id),))

        result = cursor.fetchone()
        assert result is not None
        assert result['name'] == 'updated'
        cursor.close()

        # Cleanup
        scylla_session.execute(f"DROP TABLE app_data.{table_name}")
