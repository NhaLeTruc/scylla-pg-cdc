"""
Unit tests for reconciliation comparer module.

Tests row comparison logic between ScyllaDB and PostgreSQL.
"""

import pytest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID


class TestRowComparer:
    """Test row comparison functionality."""

    @pytest.fixture
    def comparer(self):
        """Create a RowComparer instance."""
        from src.reconciliation.comparer import RowComparer
        return RowComparer()

    def test_identical_rows_are_equal(self, comparer):
        """Test that identical rows are considered equal."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "email": "test@example.com",
            "status": "active"
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "email": "test@example.com",
            "status": "active"
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_different_values_are_not_equal(self, comparer):
        """Test that rows with different values are not equal."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "email": "test@example.com",
            "status": "active"
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "email": "test@example.com",
            "status": "inactive"  # Different status
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_missing_fields_in_scylla(self, comparer):
        """Test handling of fields missing in ScyllaDB but present in Postgres."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser"
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "email": "test@example.com"  # Extra field
        }

        # Should still be equal if we only compare common fields
        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_missing_fields_in_postgres(self, comparer):
        """Test handling of fields missing in Postgres but present in ScyllaDB."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser",
            "extra_field": "value"
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "username": "testuser"
        }

        # Should still be equal if we only compare common fields
        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_null_value_comparison(self, comparer):
        """Test comparison of NULL values."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "email": None
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "email": None
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_null_vs_value_comparison(self, comparer):
        """Test comparison of NULL vs non-NULL value."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "email": None
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "email": "test@example.com"
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_timestamp_comparison_with_timezone(self, comparer):
        """Test comparison of timestamps with timezone handling."""
        timestamp1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        timestamp2 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "created_at": timestamp1
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "created_at": timestamp2
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_timestamp_comparison_different_values(self, comparer):
        """Test comparison of different timestamps."""
        timestamp1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        timestamp2 = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "created_at": timestamp1
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000",
            "created_at": timestamp2
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_decimal_comparison(self, comparer):
        """Test comparison of decimal values."""
        scylla_row = {
            "product_id": "123",
            "price": Decimal("19.99")
        }
        postgres_row = {
            "product_id": "123",
            "price": Decimal("19.99")
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_decimal_comparison_different_precision(self, comparer):
        """Test comparison of decimals with different precision."""
        scylla_row = {
            "product_id": "123",
            "price": Decimal("19.990")
        }
        postgres_row = {
            "product_id": "123",
            "price": Decimal("19.99")
        }

        # Should be equal (same value, different precision)
        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_uuid_comparison_string_format(self, comparer):
        """Test comparison of UUIDs in different formats."""
        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000"
        }
        postgres_row = {
            "user_id": UUID("123e4567-e89b-12d3-a456-426614174000")
        }

        # Should handle UUID object vs string comparison
        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_boolean_comparison(self, comparer):
        """Test comparison of boolean values."""
        scylla_row = {
            "product_id": "123",
            "is_active": True
        }
        postgres_row = {
            "product_id": "123",
            "is_active": True
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_list_comparison(self, comparer):
        """Test comparison of list/array values."""
        scylla_row = {
            "user_id": "123",
            "tags": ["tag1", "tag2", "tag3"]
        }
        postgres_row = {
            "user_id": "123",
            "tags": ["tag1", "tag2", "tag3"]
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_list_comparison_different_order(self, comparer):
        """Test comparison of lists with different order."""
        scylla_row = {
            "user_id": "123",
            "tags": ["tag1", "tag2", "tag3"]
        }
        postgres_row = {
            "user_id": "123",
            "tags": ["tag3", "tag1", "tag2"]
        }

        # Order matters for lists
        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_datetime_without_timezone(self, comparer):
        """Test datetime comparison when one has no timezone."""
        from datetime import datetime, timezone

        # Create datetime without timezone
        dt_naive = datetime(2024, 1, 15, 10, 30, 0)
        # Create datetime with timezone
        dt_aware = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        scylla_row = {
            "user_id": "123",
            "created_at": dt_naive
        }
        postgres_row = {
            "user_id": "123",
            "created_at": dt_aware
        }

        # Should match after normalization (naive assumed UTC)
        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_nested_dict_comparison(self, comparer):
        """Test comparison of nested dictionary values."""
        scylla_row = {
            "user_id": "123",
            "metadata": {"key1": "value1", "key2": "value2"}
        }
        postgres_row = {
            "user_id": "123",
            "metadata": {"key1": "value1", "key2": "value2"}
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_nested_dict_different_keys(self, comparer):
        """Test comparison of nested dicts with different keys."""
        scylla_row = {
            "user_id": "123",
            "metadata": {"key1": "value1"}
        }
        postgres_row = {
            "user_id": "123",
            "metadata": {"key2": "value2"}
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_uuid_string_to_uuid_comparison(self, comparer):
        """Test UUID string compared to UUID object."""
        from uuid import UUID

        scylla_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000"
        }
        postgres_row = {
            "user_id": UUID("123e4567-e89b-12d3-a456-426614174000")
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_uuid_to_string_comparison(self, comparer):
        """Test UUID object compared to string."""
        from uuid import UUID

        scylla_row = {
            "user_id": UUID("123e4567-e89b-12d3-a456-426614174000")
        }
        postgres_row = {
            "user_id": "123e4567-e89b-12d3-a456-426614174000"
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_uuid_to_uuid_comparison(self, comparer):
        """Test UUID object compared to UUID object."""
        from uuid import UUID

        uuid_val1 = UUID("123e4567-e89b-12d3-a456-426614174000")
        uuid_val2 = UUID("123e4567-e89b-12d3-a456-426614174000")

        scylla_row = {"user_id": uuid_val1}
        postgres_row = {"user_id": uuid_val2}

        assert comparer.compare_rows(scylla_row, postgres_row) is True

    def test_list_different_length(self, comparer):
        """Test comparison of lists with different lengths."""
        scylla_row = {
            "user_id": "123",
            "tags": ["tag1", "tag2"]
        }
        postgres_row = {
            "user_id": "123",
            "tags": ["tag1", "tag2", "tag3"]
        }

        assert comparer.compare_rows(scylla_row, postgres_row) is False

    def test_compare_with_ignore_fields(self, comparer):
        """Test comparison with ignore_fields parameter."""
        scylla_row = {
            "user_id": "123",
            "username": "testuser",
            "updated_at": "2024-01-01"
        }
        postgres_row = {
            "user_id": "123",
            "username": "testuser",
            "updated_at": "2024-01-02"
        }

        # Should be equal when ignoring updated_at
        assert comparer.compare_rows(scylla_row, postgres_row, ignore_fields=["updated_at"]) is True

    def test_get_differing_fields(self, comparer):
        """Test getting list of fields that differ."""
        scylla_row = {
            "user_id": "123",
            "username": "testuser",
            "email": "old@example.com",
            "status": "active"
        }
        postgres_row = {
            "user_id": "123",
            "username": "testuser",
            "email": "new@example.com",
            "status": "inactive"
        }

        diff = comparer.get_differing_fields(scylla_row, postgres_row)

        assert "email" in diff
        assert "status" in diff
        assert "username" not in diff
        assert diff["email"]["scylla"] == "old@example.com"
        assert diff["email"]["postgres"] == "new@example.com"
        assert diff["status"]["scylla"] == "active"
        assert diff["status"]["postgres"] == "inactive"

    def test_normalize_row(self, comparer):
        """Test row normalization for comparison."""
        row = {
            "user_id": UUID("123e4567-e89b-12d3-a456-426614174000"),
            "price": Decimal("19.990"),
            "created_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "status": "active"
        }

        normalized = comparer.normalize_row(row)

        # UUID should be string
        assert isinstance(normalized["user_id"], str)
        # Decimal should be normalized
        assert normalized["price"] == Decimal("19.99")
        # Timestamp should be UTC
        assert normalized["created_at"].tzinfo is not None

    def test_compare_rows_with_ignore_fields(self, comparer):
        """Test comparing rows while ignoring specific fields."""
        scylla_row = {
            "user_id": "123",
            "username": "testuser",
            "updated_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        }
        postgres_row = {
            "user_id": "123",
            "username": "testuser",
            "updated_at": datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
        }

        # Should be equal when ignoring updated_at
        assert comparer.compare_rows(
            scylla_row,
            postgres_row,
            ignore_fields=["updated_at"]
        ) is True

    def test_empty_rows_comparison(self, comparer):
        """Test comparison of empty rows."""
        assert comparer.compare_rows({}, {}) is True

    def test_case_sensitivity(self, comparer):
        """Test case-sensitive field name comparison."""
        scylla_row = {"UserID": "123", "Username": "test"}
        postgres_row = {"userid": "123", "username": "test"}

        # Field names should be case-sensitive by default
        result = comparer.compare_rows(scylla_row, postgres_row, case_sensitive=False)
        assert result is True

    def test_float_comparison_with_tolerance(self, comparer):
        """Test floating point comparison with tolerance."""
        scylla_row = {
            "product_id": "123",
            "rating": 4.7999999
        }
        postgres_row = {
            "product_id": "123",
            "rating": 4.8000001
        }

        # Should be equal within tolerance
        assert comparer.compare_rows(scylla_row, postgres_row, float_tolerance=0.001) is True

    def test_compare_rows_returns_detailed_result(self, comparer):
        """Test that compare_rows can return detailed comparison result."""
        scylla_row = {
            "user_id": "123",
            "username": "testuser",
            "email": "test@example.com"
        }
        postgres_row = {
            "user_id": "123",
            "username": "testuser",
            "email": "different@example.com"
        }

        result = comparer.compare_rows_detailed(scylla_row, postgres_row)

        assert result["is_equal"] is False
        assert result["matching_fields"] == ["user_id", "username"]
        assert result["differing_fields"] == ["email"]
        assert "email" in result["differences"]
