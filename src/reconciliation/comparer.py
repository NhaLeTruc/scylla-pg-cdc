"""
Row Comparer for CDC Pipeline Reconciliation

Provides row-level comparison logic between ScyllaDB and PostgreSQL data.
Handles type normalization, NULL values, and various data type edge cases.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

logger = logging.getLogger(__name__)


class RowComparer:
    """
    Compares rows from ScyllaDB and PostgreSQL.

    Handles normalization of different data types and provides
    detailed comparison results.
    """

    def __init__(self):
        """Initialize the row comparer."""
        self.float_tolerance = 0.0001  # Default tolerance for float comparison
        logger.debug("Initialized RowComparer")

    def compare_rows(
        self,
        scylla_row: Dict[str, Any],
        postgres_row: Dict[str, Any],
        ignore_fields: Optional[List[str]] = None,
        case_sensitive: bool = True,
        float_tolerance: Optional[float] = None
    ) -> bool:
        """
        Compare two rows for equality.

        Args:
            scylla_row: Row from ScyllaDB
            postgres_row: Row from PostgreSQL
            ignore_fields: List of field names to ignore in comparison
            case_sensitive: Whether field names are case-sensitive
            float_tolerance: Tolerance for floating point comparison

        Returns:
            True if rows are equal, False otherwise
        """
        if float_tolerance is not None:
            self.float_tolerance = float_tolerance

        # Normalize both rows
        norm_scylla = self.normalize_row(scylla_row)
        norm_postgres = self.normalize_row(postgres_row)

        # Get common fields (fields present in both rows)
        scylla_fields = set(norm_scylla.keys())
        postgres_fields = set(norm_postgres.keys())

        if not case_sensitive:
            # Convert to lowercase for comparison
            scylla_fields_lower = {f.lower(): f for f in scylla_fields}
            postgres_fields_lower = {f.lower(): f for f in postgres_fields}
            common_fields_lower = set(scylla_fields_lower.keys()) & set(postgres_fields_lower.keys())

            # Map back to actual field names
            common_fields = [(scylla_fields_lower[f], postgres_fields_lower[f]) for f in common_fields_lower]
        else:
            common_fields_set = scylla_fields & postgres_fields
            common_fields = [(f, f) for f in common_fields_set]

        # Apply ignore_fields filter
        if ignore_fields:
            ignore_set = set(f.lower() if not case_sensitive else f for f in ignore_fields)
            common_fields = [
                (sf, pf) for sf, pf in common_fields
                if (sf.lower() if not case_sensitive else sf) not in ignore_set
            ]

        # Compare all common fields
        for scylla_field, postgres_field in common_fields:
            scylla_value = norm_scylla[scylla_field]
            postgres_value = norm_postgres[postgres_field]

            if not self._values_equal(scylla_value, postgres_value):
                logger.debug(
                    f"Field {scylla_field} mismatch: "
                    f"ScyllaDB={scylla_value}, PostgreSQL={postgres_value}"
                )
                return False

        return True

    def compare_rows_detailed(
        self,
        scylla_row: Dict[str, Any],
        postgres_row: Dict[str, Any],
        ignore_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Compare rows and return detailed comparison result.

        Args:
            scylla_row: Row from ScyllaDB
            postgres_row: Row from PostgreSQL
            ignore_fields: Fields to ignore

        Returns:
            Dictionary with:
            - is_equal: bool
            - matching_fields: List[str]
            - differing_fields: List[str]
            - differences: Dict[str, Dict[str, Any]]
        """
        norm_scylla = self.normalize_row(scylla_row)
        norm_postgres = self.normalize_row(postgres_row)

        common_fields = set(norm_scylla.keys()) & set(norm_postgres.keys())

        if ignore_fields:
            common_fields = common_fields - set(ignore_fields)

        matching_fields = []
        differing_fields = []
        differences = {}

        for field in common_fields:
            scylla_value = norm_scylla[field]
            postgres_value = norm_postgres[field]

            if self._values_equal(scylla_value, postgres_value):
                matching_fields.append(field)
            else:
                differing_fields.append(field)
                differences[field] = {
                    "scylla": scylla_value,
                    "postgres": postgres_value
                }

        return {
            "is_equal": len(differing_fields) == 0,
            "matching_fields": sorted(matching_fields),
            "differing_fields": sorted(differing_fields),
            "differences": differences
        }

    def get_differing_fields(
        self,
        scylla_row: Dict[str, Any],
        postgres_row: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get dictionary of fields that differ between rows.

        Args:
            scylla_row: Row from ScyllaDB
            postgres_row: Row from PostgreSQL

        Returns:
            Dictionary mapping field name to {scylla: value, postgres: value}
        """
        result = self.compare_rows_detailed(scylla_row, postgres_row)
        return result["differences"]

    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize row for comparison.

        Handles:
        - UUID objects → strings
        - Decimal precision normalization
        - Timezone-aware datetime objects
        - None/NULL handling

        Args:
            row: Row dictionary

        Returns:
            Normalized row dictionary
        """
        normalized = {}

        for key, value in row.items():
            normalized[key] = self._normalize_value(value)

        return normalized

    def _normalize_value(self, value: Any) -> Any:
        """
        Normalize a single value for comparison.

        Args:
            value: Value to normalize

        Returns:
            Normalized value
        """
        # Handle None/NULL
        if value is None:
            return None

        # Handle UUID
        if isinstance(value, UUID):
            return str(value)

        # Handle Decimal - normalize precision
        if isinstance(value, Decimal):
            # Remove trailing zeros and normalize
            return value.normalize()

        # Handle datetime - ensure timezone awareness
        if isinstance(value, datetime):
            if value.tzinfo is None:
                # Assume UTC if no timezone
                return value.replace(tzinfo=timezone.utc)
            return value

        # Handle lists - ensure they're regular Python lists
        if isinstance(value, list):
            return [self._normalize_value(item) for item in value]

        # Handle dictionaries (nested objects)
        if isinstance(value, dict):
            return {k: self._normalize_value(v) for k, v in value.items()}

        # Return value as-is for other types
        return value

    def _values_equal(self, value1: Any, value2: Any) -> bool:
        """
        Compare two normalized values for equality.

        Args:
            value1: First value
            value2: Second value

        Returns:
            True if values are equal
        """
        # Handle None/NULL
        if value1 is None and value2 is None:
            return True
        if value1 is None or value2 is None:
            return False

        # Handle UUID string comparison
        if isinstance(value1, str) and isinstance(value2, UUID):
            return value1 == str(value2)
        if isinstance(value1, UUID) and isinstance(value2, str):
            return str(value1) == value2
        if isinstance(value1, UUID) and isinstance(value2, UUID):
            return str(value1) == str(value2)

        # Handle Decimal comparison
        if isinstance(value1, Decimal) and isinstance(value2, Decimal):
            # Compare normalized decimals
            return value1.normalize() == value2.normalize()

        # Handle float comparison with tolerance
        if isinstance(value1, float) and isinstance(value2, float):
            return abs(value1 - value2) < self.float_tolerance

        # Handle datetime comparison
        if isinstance(value1, datetime) and isinstance(value2, datetime):
            # Ensure both have timezone info
            v1 = value1 if value1.tzinfo else value1.replace(tzinfo=timezone.utc)
            v2 = value2 if value2.tzinfo else value2.replace(tzinfo=timezone.utc)
            return v1 == v2

        # Handle list comparison (order matters)
        if isinstance(value1, list) and isinstance(value2, list):
            if len(value1) != len(value2):
                return False
            return all(self._values_equal(v1, v2) for v1, v2 in zip(value1, value2))

        # Handle dictionary comparison
        if isinstance(value1, dict) and isinstance(value2, dict):
            if set(value1.keys()) != set(value2.keys()):
                return False
            return all(
                self._values_equal(value1[k], value2[k])
                for k in value1.keys()
            )

        # Default comparison
        return value1 == value2
