"""
Correlation ID Utility for CDC Pipeline

Provides utilities for generating and managing correlation IDs for distributed tracing
and request tracking across the CDC pipeline.
"""

import uuid
import contextvars
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Context variable for correlation ID
_correlation_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    'correlation_id',
    default=None
)


def generate_correlation_id() -> str:
    """
    Generate a new correlation ID using UUID4.

    Returns:
        String representation of a UUID4
    """
    correlation_id = str(uuid.uuid4())
    logger.debug(f"Generated correlation ID: {correlation_id}")
    return correlation_id


def get_correlation_id() -> Optional[str]:
    """
    Get the current correlation ID from context.

    Returns:
        Current correlation ID or None if not set
    """
    return _correlation_id.get()


def set_correlation_id(correlation_id: str) -> None:
    """
    Set the correlation ID in the current context.

    Args:
        correlation_id: Correlation ID to set

    Raises:
        ValueError: If correlation_id is empty or invalid
    """
    if not correlation_id or not isinstance(correlation_id, str):
        raise ValueError("Correlation ID must be a non-empty string")

    _correlation_id.set(correlation_id)
    logger.debug(f"Set correlation ID: {correlation_id}")


def get_or_create_correlation_id() -> str:
    """
    Get the current correlation ID or create a new one if not set.

    Returns:
        Current or newly created correlation ID
    """
    correlation_id = get_correlation_id()

    if not correlation_id:
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)
        logger.debug(f"Created new correlation ID: {correlation_id}")

    return correlation_id


def clear_correlation_id() -> None:
    """Clear the correlation ID from context."""
    _correlation_id.set(None)
    logger.debug("Cleared correlation ID")


class CorrelationContext:
    """
    Context manager for correlation ID management.

    Automatically creates and cleans up correlation IDs within a context.
    """

    def __init__(self, correlation_id: Optional[str] = None):
        """
        Initialize correlation context.

        Args:
            correlation_id: Optional correlation ID to use. If not provided,
                          a new one will be generated.
        """
        self.correlation_id = correlation_id
        self.previous_id = None

    def __enter__(self) -> str:
        """
        Enter the correlation context.

        Returns:
            The correlation ID for this context
        """
        self.previous_id = get_correlation_id()

        if self.correlation_id:
            set_correlation_id(self.correlation_id)
        else:
            self.correlation_id = generate_correlation_id()
            set_correlation_id(self.correlation_id)

        logger.debug(f"Entered correlation context: {self.correlation_id}")
        return self.correlation_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the correlation context and restore previous ID."""
        if self.previous_id:
            set_correlation_id(self.previous_id)
            logger.debug(f"Restored correlation ID: {self.previous_id}")
        else:
            clear_correlation_id()
            logger.debug("Cleared correlation context")


def correlation_id_filter(record):
    """
    Logging filter to add correlation ID to log records.

    Args:
        record: Log record to augment

    Returns:
        True (always allow record)
    """
    record.correlation_id = get_correlation_id() or "N/A"
    return True


def setup_correlation_logging(logger_instance: logging.Logger) -> None:
    """
    Configure a logger to include correlation IDs.

    Args:
        logger_instance: Logger instance to configure
    """
    logger_instance.addFilter(correlation_id_filter)


def extract_correlation_id_from_message(message: dict) -> Optional[str]:
    """
    Extract correlation ID from a Kafka message or similar structure.

    Args:
        message: Message dictionary with headers or metadata

    Returns:
        Correlation ID if found, None otherwise
    """
    if not isinstance(message, dict):
        return None

    # Check common locations for correlation ID
    correlation_id = message.get("correlation_id")

    if not correlation_id and "headers" in message:
        headers = message["headers"]
        if isinstance(headers, dict):
            correlation_id = headers.get("correlation_id")

    if not correlation_id and "metadata" in message:
        metadata = message["metadata"]
        if isinstance(metadata, dict):
            correlation_id = metadata.get("correlation_id")

    return correlation_id


def attach_correlation_id_to_message(message: dict) -> dict:
    """
    Attach the current correlation ID to a message.

    Args:
        message: Message dictionary to augment

    Returns:
        Augmented message with correlation ID
    """
    if not isinstance(message, dict):
        raise ValueError("Message must be a dictionary")

    correlation_id = get_or_create_correlation_id()

    # Create headers if not present
    if "headers" not in message:
        message["headers"] = {}

    if isinstance(message["headers"], dict):
        message["headers"]["correlation_id"] = correlation_id

    # Also add to metadata if present
    if "metadata" in message and isinstance(message["metadata"], dict):
        message["metadata"]["correlation_id"] = correlation_id

    logger.debug(f"Attached correlation ID to message: {correlation_id}")
    return message
