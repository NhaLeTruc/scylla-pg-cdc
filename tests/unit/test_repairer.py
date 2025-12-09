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
        assert "INSERT INTO cdc_data.users" in actions[0]["sql"]

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
        assert "DELETE FROM cdc_data.users WHERE user_id" in actions[0]["sql"]

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
        assert "UPDATE cdc_data.users SET" in actions[0]["sql"]
        assert "WHERE user_id" in actions[0]["sql"]

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
