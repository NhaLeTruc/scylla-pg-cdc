"""
Unit tests for reconciliation differ module.

Tests discrepancy detection algorithms between ScyllaDB and PostgreSQL.
"""

import pytest
from datetime import datetime, timezone


class TestDataDiffer:
    """Test discrepancy detection functionality."""

    @pytest.fixture
    def differ(self):
        """Create a DataDiffer instance."""
        from src.reconciliation.differ import DataDiffer
        return DataDiffer()

    @pytest.fixture
    def sample_scylla_data(self):
        """Sample ScyllaDB data."""
        return [
            {"user_id": "001", "username": "user1", "email": "user1@example.com", "status": "active"},
            {"user_id": "002", "username": "user2", "email": "user2@example.com", "status": "active"},
            {"user_id": "003", "username": "user3", "email": "user3@example.com", "status": "inactive"},
            {"user_id": "005", "username": "user5", "email": "user5@example.com", "status": "active"},
        ]

    @pytest.fixture
    def sample_postgres_data(self):
        """Sample PostgreSQL data."""
        return [
            {"user_id": "001", "username": "user1", "email": "user1@example.com", "status": "active"},
            {"user_id": "002", "username": "user2", "email": "updated@example.com", "status": "active"},  # Mismatch
            {"user_id": "004", "username": "user4", "email": "user4@example.com", "status": "active"},  # Extra
        ]

    def test_find_missing_in_postgres(self, differ, sample_scylla_data, sample_postgres_data):
        """Test finding rows missing in PostgreSQL."""
        missing = differ.find_missing_in_target(
            sample_scylla_data,
            sample_postgres_data,
            key_field="user_id"
        )

        assert len(missing) == 2
        user_ids = [row["user_id"] for row in missing]
        assert "003" in user_ids
        assert "005" in user_ids

    def test_find_extra_in_postgres(self, differ, sample_scylla_data, sample_postgres_data):
        """Test finding rows that exist in PostgreSQL but not in ScyllaDB."""
        extra = differ.find_extra_in_target(
            sample_scylla_data,
            sample_postgres_data,
            key_field="user_id"
        )

        assert len(extra) == 1
        assert extra[0]["user_id"] == "004"

    def test_find_mismatches(self, differ, sample_scylla_data, sample_postgres_data):
        """Test finding rows that exist in both but have different values."""
        mismatches = differ.find_mismatches(
            sample_scylla_data,
            sample_postgres_data,
            key_field="user_id"
        )

        assert len(mismatches) == 1
        assert mismatches[0]["key"] == "002"
        assert mismatches[0]["scylla"]["email"] == "user2@example.com"
        assert mismatches[0]["postgres"]["email"] == "updated@example.com"

    def test_find_all_discrepancies(self, differ, sample_scylla_data, sample_postgres_data):
        """Test finding all types of discrepancies in one call."""
        discrepancies = differ.find_all_discrepancies(
            sample_scylla_data,
            sample_postgres_data,
            key_field="user_id"
        )

        assert "missing" in discrepancies
        assert "extra" in discrepancies
        assert "mismatches" in discrepancies

        assert len(discrepancies["missing"]) == 2
        assert len(discrepancies["extra"]) == 1
        assert len(discrepancies["mismatches"]) == 1

    def test_empty_source_data(self, differ):
        """Test with empty source data."""
        scylla_data = []
        postgres_data = [
            {"user_id": "001", "username": "user1"}
        ]

        missing = differ.find_missing_in_target(scylla_data, postgres_data, key_field="user_id")
        extra = differ.find_extra_in_target(scylla_data, postgres_data, key_field="user_id")

        assert len(missing) == 0
        assert len(extra) == 1

    def test_empty_target_data(self, differ):
        """Test with empty target data."""
        scylla_data = [
            {"user_id": "001", "username": "user1"}
        ]
        postgres_data = []

        missing = differ.find_missing_in_target(scylla_data, postgres_data, key_field="user_id")
        extra = differ.find_extra_in_target(scylla_data, postgres_data, key_field="user_id")

        assert len(missing) == 1
        assert len(extra) == 0

    def test_both_empty(self, differ):
        """Test with both datasets empty."""
        discrepancies = differ.find_all_discrepancies([], [], key_field="user_id")

        assert len(discrepancies["missing"]) == 0
        assert len(discrepancies["extra"]) == 0
        assert len(discrepancies["mismatches"]) == 0

    def test_composite_key_comparison(self, differ):
        """Test comparison with composite keys."""
        scylla_data = [
            {"table_id": "1", "partition_key": "A", "value": "x"},
            {"table_id": "1", "partition_key": "B", "value": "y"},
        ]
        postgres_data = [
            {"table_id": "1", "partition_key": "A", "value": "x"},
            {"table_id": "1", "partition_key": "C", "value": "z"},
        ]

        missing = differ.find_missing_in_target(
            scylla_data,
            postgres_data,
            key_field=["table_id", "partition_key"]
        )

        assert len(missing) == 1
        assert missing[0]["partition_key"] == "B"

    def test_ignore_fields_in_comparison(self, differ):
        """Test ignoring specific fields during comparison."""
        scylla_data = [
            {"user_id": "001", "username": "user1", "updated_at": datetime(2024, 1, 1, tzinfo=timezone.utc)}
        ]
        postgres_data = [
            {"user_id": "001", "username": "user1", "updated_at": datetime(2024, 1, 2, tzinfo=timezone.utc)}
        ]

        mismatches = differ.find_mismatches(
            scylla_data,
            postgres_data,
            key_field="user_id",
            ignore_fields=["updated_at"]
        )

        # Should not detect mismatch when ignoring updated_at
        assert len(mismatches) == 0

    def test_get_discrepancy_summary(self, differ, sample_scylla_data, sample_postgres_data):
        """Test getting summary statistics of discrepancies."""
        summary = differ.get_discrepancy_summary(
            sample_scylla_data,
            sample_postgres_data,
            key_field="user_id"
        )

        assert summary["total_source_rows"] == 4
        assert summary["total_target_rows"] == 3
        assert summary["missing_count"] == 2
        assert summary["extra_count"] == 1
        assert summary["mismatch_count"] == 1
        assert summary["match_count"] == 1

    def test_detect_duplicates_in_source(self, differ):
        """Test detection of duplicate keys in source data."""
        scylla_data = [
            {"user_id": "001", "username": "user1"},
            {"user_id": "001", "username": "duplicate"},  # Duplicate
            {"user_id": "002", "username": "user2"},
        ]

        duplicates = differ.find_duplicates(scylla_data, key_field="user_id")

        assert len(duplicates) == 1
        assert duplicates[0]["key"] == "001"
        assert duplicates[0]["count"] == 2

    def test_batch_comparison(self, differ):
        """Test comparing data in batches for memory efficiency."""
        # Large dataset simulation
        scylla_data = [{"user_id": str(i), "value": f"val{i}"} for i in range(1000)]
        postgres_data = [{"user_id": str(i), "value": f"val{i}"} for i in range(500, 1500)]

        discrepancies = differ.find_all_discrepancies_batched(
            scylla_data,
            postgres_data,
            key_field="user_id",
            batch_size=100
        )

        assert discrepancies["missing_count"] == 500  # 0-499 missing in postgres
        assert discrepancies["extra_count"] == 500    # 1000-1499 extra in postgres
        assert discrepancies["mismatch_count"] == 0   # 500-999 match

    def test_find_mismatches_with_field_details(self, differ):
        """Test mismatch detection with detailed field-level differences."""
        scylla_data = [
            {"user_id": "001", "username": "user1", "email": "old@example.com", "status": "active"}
        ]
        postgres_data = [
            {"user_id": "001", "username": "user1", "email": "new@example.com", "status": "inactive"}
        ]

        mismatches = differ.find_mismatches_detailed(
            scylla_data,
            postgres_data,
            key_field="user_id"
        )

        assert len(mismatches) == 1
        mismatch = mismatches[0]
        assert "email" in mismatch["differing_fields"]
        assert "status" in mismatch["differing_fields"]
        assert "username" not in mismatch["differing_fields"]
        assert len(mismatch["differing_fields"]) == 2

    def test_null_handling_in_comparison(self, differ):
        """Test proper handling of NULL values in comparisons."""
        scylla_data = [
            {"user_id": "001", "email": None, "phone": "123-456"}
        ]
        postgres_data = [
            {"user_id": "001", "email": None, "phone": "123-456"}
        ]

        mismatches = differ.find_mismatches(scylla_data, postgres_data, key_field="user_id")
        assert len(mismatches) == 0

    def test_null_vs_empty_string(self, differ):
        """Test distinguishing between NULL and empty string."""
        scylla_data = [
            {"user_id": "001", "description": None}
        ]
        postgres_data = [
            {"user_id": "001", "description": ""}
        ]

        mismatches = differ.find_mismatches(scylla_data, postgres_data, key_field="user_id")

        # NULL and empty string should be treated as different
        assert len(mismatches) == 1

    def test_case_insensitive_key_comparison(self, differ):
        """Test case-insensitive key field comparison."""
        scylla_data = [
            {"email": "USER@EXAMPLE.COM", "name": "User"}
        ]
        postgres_data = [
            {"email": "user@example.com", "name": "User"}
        ]

        missing = differ.find_missing_in_target(
            scylla_data,
            postgres_data,
            key_field="email",
            case_sensitive_keys=False
        )

        # Should match when case-insensitive
        assert len(missing) == 0

    def test_get_row_by_key(self, differ, sample_postgres_data):
        """Test helper method to get row by key value."""
        row = differ.get_row_by_key(sample_postgres_data, "user_id", "002")

        assert row is not None
        assert row["username"] == "user2"

    def test_get_row_by_composite_key(self, differ):
        """Test getting row by composite key."""
        data = [
            {"org_id": "1", "user_id": "A", "name": "Alice"},
            {"org_id": "1", "user_id": "B", "name": "Bob"},
            {"org_id": "2", "user_id": "A", "name": "Anna"},
        ]

        row = differ.get_row_by_key(data, ["org_id", "user_id"], ["1", "B"])

        assert row is not None
        assert row["name"] == "Bob"

    def test_build_key_index(self, differ):
        """Test building index for fast key lookups."""
        data = [
            {"user_id": "001", "name": "Alice"},
            {"user_id": "002", "name": "Bob"},
            {"user_id": "003", "name": "Charlie"},
        ]

        index = differ.build_key_index(data, "user_id")

        assert len(index) == 3
        assert "001" in index
        assert index["002"]["name"] == "Bob"

    def test_percentage_calculation(self, differ):
        """Test accuracy percentage calculation."""
        discrepancies = {
            "missing": [{"user_id": "001"}],
            "extra": [],
            "mismatches": [{"key": "002"}]
        }

        total_source = 10
        percentage = differ.calculate_match_percentage(discrepancies, total_source)

        # 8 out of 10 match (2 have issues)
        assert percentage == 80.0

    def test_find_schema_differences(self, differ):
        """Test finding schema differences between datasets."""
        scylla_data = [
            {"user_id": "001", "username": "user1", "email": "user1@example.com"}
        ]
        postgres_data = [
            {"user_id": "001", "username": "user1", "phone": "123-456"}
        ]

        schema_diff = differ.find_schema_differences(scylla_data, postgres_data)

        assert "email" in schema_diff["only_in_source"]
        assert "phone" in schema_diff["only_in_target"]
        assert "user_id" in schema_diff["common_fields"]
        assert "username" in schema_diff["common_fields"]

    # ===== Bug #2 Tests: Key Validation =====

    def test_extract_key_missing_field_raises_error(self, differ):
        """Test that missing key field raises KeyError."""
        row = {"name": "John", "email": "john@example.com"}

        with pytest.raises(KeyError) as exc_info:
            differ._extract_key(row, "user_id")

        assert "Key field 'user_id' not found" in str(exc_info.value)
        assert "Available fields:" in str(exc_info.value)

    def test_extract_key_null_value_raises_error(self, differ):
        """Test that NULL key value raises ValueError."""
        row = {"user_id": None, "name": "John"}

        with pytest.raises(ValueError) as exc_info:
            differ._extract_key(row, "user_id")

        assert "Key field 'user_id' has NULL value" in str(exc_info.value)
        assert "Keys cannot be NULL" in str(exc_info.value)

    def test_extract_composite_key_missing_field(self, differ):
        """Test composite key with missing field."""
        row = {"order_id": 123, "customer_id": 456}

        with pytest.raises(KeyError) as exc_info:
            differ._extract_key(row, ["order_id", "item_id"])

        assert "Key field 'item_id' not found" in str(exc_info.value)

    def test_extract_composite_key_null_value(self, differ):
        """Test composite key with NULL value."""
        row = {"order_id": 123, "item_id": None}

        with pytest.raises(ValueError) as exc_info:
            differ._extract_key(row, ["order_id", "item_id"])

        assert "Key field 'item_id' has NULL value" in str(exc_info.value)

    def test_build_key_index_with_invalid_row(self, differ):
        """Test that build_key_index fails fast on invalid row."""
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"name": "Charlie"},  # Missing 'id'
            {"id": 4, "name": "David"}
        ]

        with pytest.raises(ValueError) as exc_info:
            differ.build_key_index(data, "id")

        assert "Invalid row at index 2" in str(exc_info.value)
        assert "Key field 'id' not found" in str(exc_info.value)

    def test_find_discrepancies_with_null_keys(self, differ):
        """Test discrepancy detection handles null keys gracefully."""
        source = [
            {"id": 1, "value": "A"},
            {"id": None, "value": "B"}  # NULL key
        ]

        target = [
            {"id": 1, "value": "A"}
        ]

        with pytest.raises(ValueError):
            differ.find_all_discrepancies(source, target, "id")

    # ===== Bug #5 Tests: Streaming Reconciliation =====

    def test_find_all_discrepancies_streaming(self, differ):
        """Test streaming discrepancy detection for large datasets."""
        # Create large dataset
        source_data = [
            {"id": i, "value": f"value_{i}", "data": f"data_{i}"}
            for i in range(1000)
        ]

        target_data = [
            {"id": i, "value": f"value_{i}", "data": f"modified_{i}" if i % 10 == 0 else f"data_{i}"}
            for i in range(500, 1500)
        ]

        result = differ.find_all_discrepancies_streaming(
            source_data,
            target_data,
            key_field="id",
            batch_size=100
        )

        # Validate structure
        assert "missing" in result
        assert "extra" in result
        assert "mismatches" in result
        assert "stats" in result

        # Validate statistics
        assert result["stats"]["total_source_rows"] == 1000
        assert result["stats"]["total_target_rows"] == 1000
        assert result["stats"]["batch_size"] == 100

        # Missing: rows 0-499 in source but not in target
        assert len(result["missing"]) == 500

        # Extra: rows 1000-1499 in target but not in source
        assert len(result["extra"]) == 500

        # Mismatches: rows 500-999 with modified data (every 10th row)
        # 500-999 = 500 common rows, 50 have mismatches (every 10th)
        assert len(result["mismatches"]) == 50

    def test_iter_discrepancies_generator(self, differ):
        """Test generator-based discrepancy iterator."""
        source_data = [
            {"id": 1, "value": "A"},
            {"id": 2, "value": "B"},
            {"id": 3, "value": "C"},
        ]

        target_data = [
            {"id": 2, "value": "B_modified"},  # Mismatch
            {"id": 3, "value": "C"},
            {"id": 4, "value": "D"},  # Extra
        ]

        discrepancies = list(differ.iter_discrepancies(
            source_data,
            target_data,
            key_field="id",
            batch_size=2
        ))

        # Count by type
        missing_count = sum(1 for t, _ in discrepancies if t == "missing")
        extra_count = sum(1 for t, _ in discrepancies if t == "extra")
        mismatch_count = sum(1 for t, _ in discrepancies if t == "mismatch")

        assert missing_count == 1  # id=1
        assert extra_count == 1    # id=4
        assert mismatch_count == 1 # id=2

    def test_iter_discrepancies_large_dataset(self, differ):
        """Test iterator with large dataset for memory efficiency."""
        # Create 10,000 row dataset
        source_data = [{"id": i, "value": f"val_{i}"} for i in range(10000)]
        target_data = [{"id": i, "value": f"val_{i}"} for i in range(5000, 15000)]

        # Process as iterator
        missing_count = 0
        extra_count = 0
        mismatch_count = 0

        for disc_type, disc_data in differ.iter_discrepancies(
            source_data,
            target_data,
            key_field="id",
            batch_size=1000
        ):
            if disc_type == "missing":
                missing_count += 1
            elif disc_type == "extra":
                extra_count += 1
            elif disc_type == "mismatch":
                mismatch_count += 1

        assert missing_count == 5000  # 0-4999
        assert extra_count == 5000    # 10000-14999
        assert mismatch_count == 0    # All common rows match

    def test_streaming_with_mismatches(self, differ):
        """Test streaming detection with field-level mismatches."""
        source_data = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
        ]

        target_data = [
            {"id": 1, "name": "Alice", "email": "alice@newdomain.com"},  # Mismatch
            {"id": 2, "name": "Robert", "email": "bob@example.com"},     # Mismatch
            {"id": 3, "name": "Charlie", "email": "charlie@example.com"}, # Match
        ]

        result = differ.find_all_discrepancies_streaming(
            source_data,
            target_data,
            key_field="id",
            batch_size=2
        )

        assert len(result["missing"]) == 0
        assert len(result["extra"]) == 0
        assert len(result["mismatches"]) == 2

        # Verify mismatch structure
        mismatch = result["mismatches"][0]
        assert "key" in mismatch
        assert "scylla" in mismatch
        assert "postgres" in mismatch

    def test_streaming_with_ignore_fields(self, differ):
        """Test streaming with ignored fields."""
        source_data = [
            {"id": 1, "value": "A", "timestamp": "2024-01-01"},
            {"id": 2, "value": "B", "timestamp": "2024-01-02"},
        ]

        target_data = [
            {"id": 1, "value": "A", "timestamp": "2024-12-01"},  # timestamp different
            {"id": 2, "value": "B", "timestamp": "2024-12-02"},  # timestamp different
        ]

        # Without ignoring timestamp - should find mismatches
        result1 = differ.find_all_discrepancies_streaming(
            source_data,
            target_data,
            key_field="id"
        )
        assert len(result1["mismatches"]) == 2

        # With ignoring timestamp - should find no mismatches
        result2 = differ.find_all_discrepancies_streaming(
            source_data,
            target_data,
            key_field="id",
            ignore_fields=["timestamp"]
        )
        assert len(result2["mismatches"]) == 0

    def test_iter_discrepancies_empty_datasets(self, differ):
        """Test iterator with empty datasets."""
        result = list(differ.iter_discrepancies([], [], key_field="id"))
        assert len(result) == 0

    def test_streaming_batch_progress_logging(self, differ):
        """Test that batch processing logs progress correctly."""
        # Create dataset large enough to trigger progress logging
        source_data = [{"id": i, "value": f"v{i}"} for i in range(15000)]
        target_data = [{"id": i, "value": f"v{i}"} for i in range(15000)]

        result = differ.find_all_discrepancies_streaming(
            source_data,
            target_data,
            key_field="id",
            batch_size=1000
        )

        # Verify stats
        assert result["stats"]["batches_processed"] == 15  # 15000 / 1000
        assert result["stats"]["batch_size"] == 1000
        assert len(result["missing"]) == 0
        assert len(result["extra"]) == 0
        assert len(result["mismatches"]) == 0
