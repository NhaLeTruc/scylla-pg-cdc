"""
Unit tests for correlation module.
"""

import pytest
import uuid
from src.utils.correlation import (
    generate_correlation_id,
    get_correlation_id,
    set_correlation_id,
    get_or_create_correlation_id,
    clear_correlation_id,
    CorrelationContext,
    extract_correlation_id_from_message,
    attach_correlation_id_to_message
)


class TestCorrelationIdGeneration:
    """Test correlation ID generation functions."""

    def test_generate_correlation_id_returns_valid_uuid(self):
        """Test that generated correlation ID is a valid UUID."""
        correlation_id = generate_correlation_id()

        assert isinstance(correlation_id, str)
        assert len(correlation_id) == 36

        # Verify it's a valid UUID
        uuid_obj = uuid.UUID(correlation_id)
        assert str(uuid_obj) == correlation_id

    def test_generate_correlation_id_returns_unique_values(self):
        """Test that multiple generated IDs are unique."""
        id1 = generate_correlation_id()
        id2 = generate_correlation_id()
        id3 = generate_correlation_id()

        assert id1 != id2
        assert id2 != id3
        assert id1 != id3


class TestCorrelationIdContext:
    """Test correlation ID context management."""

    def setup_method(self):
        """Clear correlation ID before each test."""
        clear_correlation_id()

    def teardown_method(self):
        """Clear correlation ID after each test."""
        clear_correlation_id()

    def test_get_correlation_id_returns_none_when_not_set(self):
        """Test that get returns None when ID is not set."""
        assert get_correlation_id() is None

    def test_set_and_get_correlation_id(self):
        """Test setting and retrieving correlation ID."""
        test_id = "test-correlation-id-123"
        set_correlation_id(test_id)

        assert get_correlation_id() == test_id

    def test_set_correlation_id_with_empty_string_raises_error(self):
        """Test that empty string raises ValueError."""
        with pytest.raises(ValueError, match="non-empty string"):
            set_correlation_id("")

    def test_set_correlation_id_with_none_raises_error(self):
        """Test that None raises ValueError."""
        with pytest.raises(ValueError, match="non-empty string"):
            set_correlation_id(None)

    def test_set_correlation_id_with_non_string_raises_error(self):
        """Test that non-string value raises ValueError."""
        with pytest.raises(ValueError, match="non-empty string"):
            set_correlation_id(12345)

    def test_clear_correlation_id(self):
        """Test clearing correlation ID."""
        set_correlation_id("test-id")
        assert get_correlation_id() == "test-id"

        clear_correlation_id()
        assert get_correlation_id() is None

    def test_get_or_create_correlation_id_creates_new_id(self):
        """Test that get_or_create creates new ID when none exists."""
        correlation_id = get_or_create_correlation_id()

        assert correlation_id is not None
        assert len(correlation_id) == 36
        assert get_correlation_id() == correlation_id

    def test_get_or_create_correlation_id_returns_existing_id(self):
        """Test that get_or_create returns existing ID."""
        existing_id = "existing-id-456"
        set_correlation_id(existing_id)

        correlation_id = get_or_create_correlation_id()

        assert correlation_id == existing_id


class TestCorrelationContext:
    """Test CorrelationContext context manager."""

    def setup_method(self):
        """Clear correlation ID before each test."""
        clear_correlation_id()

    def teardown_method(self):
        """Clear correlation ID after each test."""
        clear_correlation_id()

    def test_context_creates_new_id(self):
        """Test that context manager creates new ID."""
        with CorrelationContext() as correlation_id:
            assert correlation_id is not None
            assert get_correlation_id() == correlation_id

        # Context should be cleared after exit
        assert get_correlation_id() is None

    def test_context_uses_provided_id(self):
        """Test that context manager uses provided ID."""
        test_id = "custom-id-789"

        with CorrelationContext(test_id) as correlation_id:
            assert correlation_id == test_id
            assert get_correlation_id() == test_id

    def test_context_restores_previous_id(self):
        """Test that context manager restores previous ID."""
        original_id = "original-id"
        set_correlation_id(original_id)

        with CorrelationContext() as nested_id:
            assert get_correlation_id() == nested_id
            assert nested_id != original_id

        # Original ID should be restored
        assert get_correlation_id() == original_id

    def test_nested_contexts(self):
        """Test nested correlation contexts."""
        with CorrelationContext("outer") as outer_id:
            assert get_correlation_id() == "outer"

            with CorrelationContext("inner") as inner_id:
                assert get_correlation_id() == "inner"
                assert inner_id != outer_id

            # Outer context should be restored
            assert get_correlation_id() == "outer"

        assert get_correlation_id() is None

    def test_context_clears_on_exception(self):
        """Test that context is cleaned up even on exception."""
        try:
            with CorrelationContext() as correlation_id:
                assert get_correlation_id() == correlation_id
                raise RuntimeError("Test exception")
        except RuntimeError:
            pass

        assert get_correlation_id() is None


class TestMessageCorrelation:
    """Test message correlation functions."""

    def setup_method(self):
        """Clear correlation ID before each test."""
        clear_correlation_id()

    def teardown_method(self):
        """Clear correlation ID after each test."""
        clear_correlation_id()

    def test_extract_correlation_id_from_root_level(self):
        """Test extracting correlation ID from root level of message."""
        message = {"correlation_id": "test-id-123", "data": "test"}

        correlation_id = extract_correlation_id_from_message(message)
        assert correlation_id == "test-id-123"

    def test_extract_correlation_id_from_headers(self):
        """Test extracting correlation ID from headers."""
        message = {
            "headers": {"correlation_id": "header-id-456"},
            "data": "test"
        }

        correlation_id = extract_correlation_id_from_message(message)
        assert correlation_id == "header-id-456"

    def test_extract_correlation_id_from_metadata(self):
        """Test extracting correlation ID from metadata."""
        message = {
            "metadata": {"correlation_id": "metadata-id-789"},
            "data": "test"
        }

        correlation_id = extract_correlation_id_from_message(message)
        assert correlation_id == "metadata-id-789"

    def test_extract_correlation_id_not_found(self):
        """Test extracting correlation ID when not present."""
        message = {"data": "test"}

        correlation_id = extract_correlation_id_from_message(message)
        assert correlation_id is None

    def test_extract_correlation_id_from_non_dict(self):
        """Test extracting from non-dictionary returns None."""
        correlation_id = extract_correlation_id_from_message("not a dict")
        assert correlation_id is None

    def test_attach_correlation_id_to_message(self):
        """Test attaching correlation ID to message."""
        set_correlation_id("attached-id-123")
        message = {"data": "test"}

        result = attach_correlation_id_to_message(message)

        assert "headers" in result
        assert result["headers"]["correlation_id"] == "attached-id-123"

    def test_attach_correlation_id_creates_new_if_not_set(self):
        """Test that attaching creates new ID if not set."""
        message = {"data": "test"}

        result = attach_correlation_id_to_message(message)

        assert "headers" in result
        assert "correlation_id" in result["headers"]
        assert len(result["headers"]["correlation_id"]) == 36

    def test_attach_correlation_id_preserves_existing_headers(self):
        """Test that attaching preserves existing headers."""
        set_correlation_id("new-id")
        message = {
            "headers": {"existing": "value"},
            "data": "test"
        }

        result = attach_correlation_id_to_message(message)

        assert result["headers"]["existing"] == "value"
        assert result["headers"]["correlation_id"] == "new-id"

    def test_attach_correlation_id_to_metadata(self):
        """Test attaching correlation ID to metadata if present."""
        set_correlation_id("metadata-id")
        message = {
            "metadata": {"key": "value"},
            "data": "test"
        }

        result = attach_correlation_id_to_message(message)

        assert result["metadata"]["correlation_id"] == "metadata-id"
        assert result["metadata"]["key"] == "value"

    def test_attach_correlation_id_to_non_dict_raises_error(self):
        """Test that attaching to non-dict raises ValueError."""
        with pytest.raises(ValueError, match="must be a dictionary"):
            attach_correlation_id_to_message("not a dict")

    def test_correlation_id_filter_when_no_id(self):
        """Test correlation_id_filter when no correlation ID is set."""
        from src.utils.correlation import correlation_id_filter
        import logging

        clear_correlation_id()

        # Create a mock record
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )

        # Should set to "N/A" when no correlation ID exists
        result = correlation_id_filter(record)
        assert result is True
        assert record.correlation_id == "N/A"

    def test_setup_correlation_logging(self):
        """Test setup_correlation_logging adds filter to logger."""
        from src.utils.correlation import setup_correlation_logging
        import logging

        logger = logging.getLogger("test_logger")
        initial_filter_count = len(logger.filters)

        setup_correlation_logging(logger)

        # Should add one filter
        assert len(logger.filters) == initial_filter_count + 1
