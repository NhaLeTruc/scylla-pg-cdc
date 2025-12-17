"""
Unit tests for reconciliation repairer module.

Tests repair action generation for INSERT/UPDATE/DELETE operations.
"""

import pytest


class TestDataRepairer:
    """Test repair action generation functionality."""

    @pytest.fixture
    def repairer(self):
        """Create a DataRepairer instance."""
        from src.reconciliation.repairer import DataRepairer
        return DataRepairer()

    @pytest.fixture
    def sample_discrepancies(self):
        """Sample discrepancies for testing."""
        return {
            "missing": [
                {"user_id": "003", "username": "user3", "email": "user3@example.com", "status": "inactive"},
                {"user_id": "005", "username": "user5", "email": "user5@example.com", "status": "active"},
            ],
            "extra": [
                {"user_id": "004", "username": "user4", "email": "user4@example.com", "status": "active"},
            ],
            "mismatches": [
                {
                    "key": "002",
                    "scylla": {"user_id": "002", "username": "user2", "email": "user2@example.com", "status": "active"},
                    "postgres": {"user_id": "002", "username": "user2", "email": "old@example.com", "status": "active"}
                }
            ]
        }

    def test_generate_insert_actions(self, repairer, sample_discrepancies):
        """Test generating INSERT actions for missing rows."""
        actions = repairer.generate_insert_actions(
            sample_discrepancies["missing"],
            table_name="users",
            schema="cdc_data"
        )

        assert len(actions) == 2
        assert all(action["action_type"] == "INSERT" for action in actions)
        assert all(action["table"] == "cdc_data.users" for action in actions)
        # Updated to check for quoted identifiers (Bug #1 fix)
        assert 'INSERT INTO "cdc_data"."users"' in actions[0]["sql"]

    def test_generate_delete_actions(self, repairer, sample_discrepancies):
        """Test generating DELETE actions for extra rows."""
        actions = repairer.generate_delete_actions(
            sample_discrepancies["extra"],
            table_name="users",
            schema="cdc_data",
            key_field="user_id"
        )

        assert len(actions) == 1
        assert actions[0]["action_type"] == "DELETE"
        assert actions[0]["table"] == "cdc_data.users"
        # Updated to check for quoted identifiers (Bug #1 fix)
        assert 'DELETE FROM "cdc_data"."users" WHERE "user_id"' in actions[0]["sql"]

    def test_generate_update_actions(self, repairer, sample_discrepancies):
        """Test generating UPDATE actions for mismatched rows."""
        actions = repairer.generate_update_actions(
            sample_discrepancies["mismatches"],
            table_name="users",
            schema="cdc_data",
            key_field="user_id"
        )

        assert len(actions) == 1
        assert actions[0]["action_type"] == "UPDATE"
        assert actions[0]["table"] == "cdc_data.users"
        # Updated to check for quoted identifiers (Bug #1 fix)
        assert 'UPDATE "cdc_data"."users" SET' in actions[0]["sql"]
        assert 'WHERE "user_id"' in actions[0]["sql"]

    def test_generate_all_repair_actions(self, repairer, sample_discrepancies):
        """Test generating all types of repair actions."""
        actions = repairer.generate_repair_actions(
            sample_discrepancies,
            table_name="users",
            schema="cdc_data",
            key_field="user_id"
        )

        assert len(actions) == 4  # 2 inserts + 1 delete + 1 update
        action_types = [a["action_type"] for a in actions]
        assert "INSERT" in action_types
        assert "DELETE" in action_types
        assert "UPDATE" in action_types

    def test_generate_sql_with_quoted_identifiers(self, repairer):
        """Test SQL generation with quoted identifiers."""
        missing_row = {"user_id": "123", "username": "test", "email": "test@example.com"}

        action = repairer.generate_insert_sql(
            missing_row,
            table_name="users",
            schema="cdc_data",
            quote_identifiers=True
        )

        # Should have quoted identifiers
        assert '"cdc_data"."users"' in action["sql"] or '`cdc_data`.`users`' in action["sql"]

    def test_generate_sql_with_null_values(self, repairer):
        """Test SQL generation handles NULL values correctly."""
        row = {"user_id": "123", "email": None, "phone": None}

        action = repairer.generate_insert_sql(
            row,
            table_name="users",
            schema="cdc_data"
        )

        # NULL should appear in SQL
        assert "NULL" in action["sql"]

    def test_generate_sql_escapes_strings(self, repairer):
        """Test SQL generation escapes string values properly."""
        row = {"user_id": "123", "comment": "O'Connor's test"}

        action = repairer.generate_insert_sql(
            row,
            table_name="users",
            schema="cdc_data"
        )

        # Should handle quotes properly
        assert "O''Connor" in action["sql"] or "O\\'Connor" in action["sql"]

    def test_dry_run_actions(self, repairer, sample_discrepancies):
        """Test dry-run mode doesn't execute but generates SQL."""
        actions = repairer.generate_repair_actions(
            sample_discrepancies,
            table_name="users",
            schema="cdc_data",
            key_field="user_id",
            dry_run=True
        )

        for action in actions:
            assert action["dry_run"] is True
            assert "sql" in action
            assert action["status"] == "pending"

    def test_composite_key_delete(self, repairer):
        """Test DELETE with composite keys."""
        extra_row = {"org_id": "1", "user_id": "A", "name": "Alice"}

        action = repairer.generate_delete_sql(
            extra_row,
            table_name="users",
            schema="cdc_data",
            key_field=["org_id", "user_id"]
        )

        sql = action["sql"]
        assert "org_id" in sql
        assert "user_id" in sql
        assert "WHERE" in sql
        assert "AND" in sql

    def test_composite_key_update(self, repairer):
        """Test UPDATE with composite keys."""
        mismatch = {
            "key": ("1", "A"),
            "scylla": {"org_id": "1", "user_id": "A", "name": "Alice", "status": "active"},
            "postgres": {"org_id": "1", "user_id": "A", "name": "Alice", "status": "inactive"}
        }

        action = repairer.generate_update_sql(
            mismatch,
            table_name="users",
            schema="cdc_data",
            key_field=["org_id", "user_id"]
        )

        sql = action["sql"]
        assert "UPDATE" in sql
        assert "SET" in sql
        assert "status" in sql  # Should update changed field
        assert "org_id" in sql  # Should be in WHERE
        assert "user_id" in sql  # Should be in WHERE

    def test_batch_actions_generation(self, repairer):
        """Test generating actions in batches."""
        # Create large dataset
        missing = [{"user_id": str(i), "name": f"user{i}"} for i in range(1000)]

        actions = repairer.generate_insert_actions(
            missing,
            table_name="users",
            schema="cdc_data",
            batch_size=100
        )

        # Should generate batch inserts
        assert len(actions) <= 10  # 1000 rows / 100 batch = 10 batches

    def test_validate_action_structure(self, repairer):
        """Test that generated actions have required structure."""
        row = {"user_id": "123", "username": "test"}

        action = repairer.generate_insert_sql(
            row,
            table_name="users",
            schema="cdc_data"
        )

        # Verify action structure
        assert "action_type" in action
        assert "table" in action
        assert "sql" in action
        assert "row_data" in action
        assert "timestamp" in action

    def test_generate_actions_with_metadata(self, repairer, sample_discrepancies):
        """Test actions include metadata for tracking."""
        actions = repairer.generate_repair_actions(
            sample_discrepancies,
            table_name="users",
            schema="cdc_data",
            key_field="user_id",
            include_metadata=True
        )

        for action in actions:
            assert "generated_at" in action
            assert "discrepancy_type" in action

    def test_empty_discrepancies(self, repairer):
        """Test handling of empty discrepancies."""
        empty_discrepancies = {
            "missing": [],
            "extra": [],
            "mismatches": []
        }

        actions = repairer.generate_repair_actions(
            empty_discrepancies,
            table_name="users",
            schema="cdc_data",
            key_field="user_id"
        )

        assert len(actions) == 0

    def test_prioritize_actions(self, repairer, sample_discrepancies):
        """Test action prioritization (DELETE → INSERT → UPDATE)."""
        actions = repairer.generate_repair_actions(
            sample_discrepancies,
            table_name="users",
            schema="cdc_data",
            key_field="user_id",
            prioritize=True
        )

        # Actions should be ordered: DELETE first, then INSERT, then UPDATE
        delete_indices = [i for i, a in enumerate(actions) if a["action_type"] == "DELETE"]
        insert_indices = [i for i, a in enumerate(actions) if a["action_type"] == "INSERT"]
        update_indices = [i for i, a in enumerate(actions) if a["action_type"] == "UPDATE"]

        if delete_indices and insert_indices:
            assert max(delete_indices) < min(insert_indices)
        if insert_indices and update_indices:
            assert max(insert_indices) < min(update_indices)

    #  ===== Bug #1 Tests: SQL Injection Protection =====

    def test_insert_sql_with_reserved_keywords(self, repairer):
        """Test INSERT with SQL reserved keywords as field names."""
        row = {
            "id": 1,
            "order": "ORD-123",  # Reserved keyword
            "select": "value",    # Reserved keyword
            "from": "source"      # Reserved keyword
        }

        result = repairer.generate_insert_sql(
            row=row,
            table_name="test_table",
            schema="test_schema"
        )

        # Verify keywords are quoted
        assert '"order"' in result["sql"]
        assert '"select"' in result["sql"]
        assert '"from"' in result["sql"]

        # Verify SQL is valid PostgreSQL
        assert '"test_schema"."test_table"' in result["sql"]

    def test_update_sql_with_special_characters(self, repairer):
        """Test UPDATE with special characters in field names."""
        mismatch = {
            "scylla": {
                "id": 1,
                "user-name": "john",  # Hyphen in field name
                "email@address": "john@example.com"  # Special char
            },
            "postgres": {
                "id": 1,
                "user-name": "jane",
                "email@address": "jane@example.com"
            }
        }

        result = repairer.generate_update_sql(
            mismatch=mismatch,
            table_name="users",
            schema="cdc_data",
            key_field="id"
        )

        # Verify special characters are handled
        assert '"user-name"' in result["sql"]
        assert '"email@address"' in result["sql"]

    def test_delete_sql_with_composite_key(self, repairer):
        """Test DELETE with composite key containing reserved words."""
        row = {
            "table": "orders",  # Reserved keyword
            "order": 123,       # Reserved keyword
            "value": 100.0
        }

        result = repairer.generate_delete_sql(
            row=row,
            table_name="test_table",
            schema="test_schema",
            key_field=["table", "order"]
        )

        # Verify composite key is quoted
        assert '"table"' in result["sql"]
        assert '"order"' in result["sql"]
        assert 'WHERE' in result["sql"]

    def test_quote_identifier_escapes_double_quotes(self, repairer):
        """Test that double quotes in identifiers are escaped."""
        # Field name with double quote
        quoted = repairer._quote_identifier('field"name')
        assert quoted == '"field""name"'  # Double quotes are doubled

    def test_sql_injection_prevention(self, repairer):
        """Test that SQL injection attempts are neutralized."""
        # Malicious field name
        malicious_row = {
            "id": 1,
            "'; DROP TABLE users; --": "malicious"
        }

        result = repairer.generate_insert_sql(
            row=malicious_row,
            table_name="test_table",
            schema="test_schema"
        )

        # Verify injection is neutralized by quoting
        assert 'DROP TABLE' in result["sql"]  # It's there but quoted
        assert '"\'; DROP TABLE users; --"' in result["sql"]  # Safely quoted

    def test_backward_compatibility_quote_identifiers_false(self, repairer):
        """Test backward compatibility when quote_identifiers=False."""
        row = {"id": 1, "name": "test"}

        result = repairer.generate_insert_sql(
            row=row,
            table_name="test_table",
            schema="test_schema",
            quote_identifiers=False
        )

        # Should not quote regular identifiers when disabled
        assert '"id"' not in result["sql"]
        assert '"name"' not in result["sql"]
        assert '"test_schema"."test_table"' in result["sql"]  # Table still quoted

    # ===== Bug #3 Tests: Data Type Handling =====

    def test_format_value_uuid(self, repairer):
        """Test UUID formatting."""
        from uuid import UUID
        uuid_val = UUID('123e4567-e89b-12d3-a456-426614174000')

        result = repairer._format_value(uuid_val)
        assert result == "'123e4567-e89b-12d3-a456-426614174000'"

    def test_format_value_bytes(self, repairer):
        """Test bytes formatting for PostgreSQL bytea."""
        # Binary data
        binary_data = b'\x00\x01\x02\xDE\xAD\xBE\xEF'
        result = repairer._format_value(binary_data)

        assert result == "'\\x000102deadbeef'"

    def test_format_value_bytearray(self, repairer):
        """Test bytearray formatting."""
        data = bytearray([0, 1, 2, 255])
        result = repairer._format_value(data)

        assert result == "'\\x000102ff'"

    def test_format_value_timedelta(self, repairer):
        """Test timedelta formatting as PostgreSQL interval."""
        from datetime import timedelta

        # 1 hour, 30 minutes
        delta = timedelta(hours=1, minutes=30)
        result = repairer._format_value(delta)

        assert result == "INTERVAL '5400 seconds'"

    def test_format_value_decimal(self, repairer):
        """Test Decimal formatting."""
        from decimal import Decimal

        decimal_val = Decimal('123.456')
        result = repairer._format_value(decimal_val)

        assert result == "123.456"

    def test_format_value_large_number(self, repairer):
        """Test very large number formatting."""
        large_num = 9999999999999999999999999999
        result = repairer._format_value(large_num)

        assert result == "9999999999999999999999999999"

    def test_format_value_unsupported_type_raises_error(self, repairer):
        """Test that unsupported types raise TypeError."""
        # Custom class
        class CustomObject:
            pass

        obj = CustomObject()

        with pytest.raises(TypeError) as exc_info:
            repairer._format_value(obj)

        assert "Unsupported data type" in str(exc_info.value)
        assert "CustomObject" in str(exc_info.value)
        assert "Supported types:" in str(exc_info.value)

    def test_insert_with_binary_data(self, repairer):
        """Integration test: INSERT with binary data."""
        from uuid import UUID

        row = {
            "id": 1,
            "data": b'\x89PNG\r\n\x1a\n',  # PNG header
            "checksum": UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')
        }

        result = repairer.generate_insert_sql(
            row=row,
            table_name="files",
            schema="storage"
        )

        assert "'\\x89504e470d0a1a0a'" in result["sql"]
        assert "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'" in result["sql"]

    def test_update_with_timedelta(self, repairer):
        """Integration test: UPDATE with timedelta."""
        from datetime import timedelta

        mismatch = {
            "scylla": {
                "id": 1,
                "duration": timedelta(minutes=45)
            },
            "postgres": {
                "id": 1,
                "duration": timedelta(minutes=30)
            }
        }

        result = repairer.generate_update_sql(
            mismatch=mismatch,
            table_name="sessions",
            schema="app_data",
            key_field="id"
        )

        assert "INTERVAL '2700 seconds'" in result["sql"]

    def test_generate_batch_insert_empty_rows_error(self, repairer):
        """Test that batch insert with empty rows raises ValueError."""
        with pytest.raises(ValueError, match="Cannot generate batch insert for empty rows"):
            repairer._generate_batch_insert_sql(
                rows=[],
                table_name="users",
                schema="cdc_data"
            )

    def test_generate_insert_actions_with_batch_size(self, repairer):
        """Test generating batch INSERT actions."""
        missing_rows = [
            {"user_id": "001", "username": "user1", "email": "user1@example.com"},
            {"user_id": "002", "username": "user2", "email": "user2@example.com"},
            {"user_id": "003", "username": "user3", "email": "user3@example.com"},
        ]

        actions = repairer.generate_insert_actions(
            missing_rows,
            table_name="users",
            schema="cdc_data",
            batch_size=2
        )

        # Should create 2 batch actions (2+1)
        assert len(actions) == 2
        assert actions[0]["batch_size"] == 2
        assert actions[1]["batch_size"] == 1

    def test_generate_update_with_composite_key(self, repairer):
        """Test UPDATE generation with composite key."""
        mismatch = {
            "scylla": {"region": "US", "user_id": "123", "status": "active"},
            "postgres": {"region": "US", "user_id": "123", "status": "inactive"}
        }

        result = repairer.generate_update_sql(
            mismatch=mismatch,
            table_name="users",
            schema="cdc_data",
            key_field=["region", "user_id"]
        )

        # Should have composite WHERE clause
        assert '"region"' in result["sql"]
        assert '"user_id"' in result["sql"]
        assert "AND" in result["sql"]

    def test_generate_update_with_no_differing_fields(self, repairer):
        """Test UPDATE when no specific differing fields detected."""
        # Create mismatch where values appear equal but should still update non-key fields
        mismatch = {
            "scylla": {"user_id": "123", "username": "test", "email": "test@example.com"},
            "postgres": {"user_id": "123", "username": "test", "email": "test@example.com"}
        }

        result = repairer.generate_update_sql(
            mismatch=mismatch,
            table_name="users",
            schema="cdc_data",
            key_field="user_id"
        )

        # Should update all non-key fields when no diff detected
        assert "UPDATE" in result["sql"]
        assert '"username"' in result["sql"] or '"email"' in result["sql"]
