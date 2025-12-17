"""
Data Differ for CDC Pipeline Reconciliation

Detects discrepancies between ScyllaDB and PostgreSQL datasets.
Identifies missing rows, extra rows, and mismatched data.
"""

import logging
from typing import Dict, List, Any, Union, Optional, Tuple
from collections import defaultdict

from src.reconciliation.comparer import RowComparer

logger = logging.getLogger(__name__)


class DataDiffer:
    """
    Detects discrepancies between source and target datasets.

    Identifies:
    - Missing rows (in source but not in target)
    - Extra rows (in target but not in source)
    - Mismatched rows (different values)
    """

    def __init__(self):
        """Initialize the data differ."""
        self.comparer = RowComparer()
        logger.debug("Initialized DataDiffer")

    def find_missing_in_target(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        case_sensitive_keys: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Find rows that exist in source but not in target.

        Args:
            source_data: Source dataset (ScyllaDB)
            target_data: Target dataset (PostgreSQL)
            key_field: Field name(s) to use as primary key
            case_sensitive_keys: Whether keys are case-sensitive

        Returns:
            List of missing rows
        """
        source_index = self.build_key_index(source_data, key_field, case_sensitive_keys)
        target_index = self.build_key_index(target_data, key_field, case_sensitive_keys)

        missing_keys = set(source_index.keys()) - set(target_index.keys())

        missing_rows = [source_index[key] for key in missing_keys]

        logger.info(f"Found {len(missing_rows)} rows missing in target")
        return missing_rows

    def find_extra_in_target(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Find rows that exist in target but not in source.

        Args:
            source_data: Source dataset (ScyllaDB)
            target_data: Target dataset (PostgreSQL)
            key_field: Field name(s) to use as primary key

        Returns:
            List of extra rows
        """
        source_index = self.build_key_index(source_data, key_field)
        target_index = self.build_key_index(target_data, key_field)

        extra_keys = set(target_index.keys()) - set(source_index.keys())

        extra_rows = [target_index[key] for key in extra_keys]

        logger.info(f"Found {len(extra_rows)} extra rows in target")
        return extra_rows

    def find_mismatches(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        ignore_fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find rows that exist in both but have different values.

        Args:
            source_data: Source dataset (ScyllaDB)
            target_data: Target dataset (PostgreSQL)
            key_field: Field name(s) to use as primary key
            ignore_fields: Fields to ignore in comparison

        Returns:
            List of mismatches with structure:
            [{"key": key_value, "scylla": row, "postgres": row}, ...]
        """
        source_index = self.build_key_index(source_data, key_field)
        target_index = self.build_key_index(target_data, key_field)

        common_keys = set(source_index.keys()) & set(target_index.keys())

        mismatches = []

        for key in common_keys:
            source_row = source_index[key]
            target_row = target_index[key]

            if not self.comparer.compare_rows(source_row, target_row, ignore_fields=ignore_fields):
                mismatches.append({
                    "key": key,
                    "scylla": source_row,
                    "postgres": target_row
                })

        logger.info(f"Found {len(mismatches)} mismatched rows")
        return mismatches

    def find_mismatches_detailed(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        ignore_fields: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Find mismatches with detailed field-level differences.

        Args:
            source_data: Source dataset
            target_data: Target dataset
            key_field: Key field(s)
            ignore_fields: Fields to ignore

        Returns:
            List of detailed mismatches
        """
        source_index = self.build_key_index(source_data, key_field)
        target_index = self.build_key_index(target_data, key_field)

        common_keys = set(source_index.keys()) & set(target_index.keys())

        mismatches = []

        for key in common_keys:
            source_row = source_index[key]
            target_row = target_index[key]

            comparison = self.comparer.compare_rows_detailed(
                source_row,
                target_row,
                ignore_fields=ignore_fields
            )

            if not comparison["is_equal"]:
                mismatches.append({
                    "key": key,
                    "scylla": source_row,
                    "postgres": target_row,
                    "differing_fields": comparison["differing_fields"],
                    "differences": comparison["differences"]
                })

        return mismatches

    def find_all_discrepancies(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        ignore_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Find all types of discrepancies in one call.

        Args:
            source_data: Source dataset
            target_data: Target dataset
            key_field: Key field(s)
            ignore_fields: Fields to ignore in mismatch detection

        Returns:
            Dictionary with:
            - missing: List of rows missing in target
            - extra: List of extra rows in target
            - mismatches: List of mismatched rows
        """
        logger.info("Finding all discrepancies...")

        missing = self.find_missing_in_target(source_data, target_data, key_field)
        extra = self.find_extra_in_target(source_data, target_data, key_field)
        mismatches = self.find_mismatches(source_data, target_data, key_field, ignore_fields)

        logger.info(
            f"Discrepancy summary: {len(missing)} missing, "
            f"{len(extra)} extra, {len(mismatches)} mismatched"
        )

        return {
            "missing": missing,
            "extra": extra,
            "mismatches": mismatches
        }

    def find_all_discrepancies_batched(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        batch_size: int = 1000,
        ignore_fields: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Find discrepancies in batches for memory efficiency.

        Args:
            source_data: Source dataset
            target_data: Target dataset
            key_field: Key field(s)
            batch_size: Number of rows per batch
            ignore_fields: Fields to ignore

        Returns:
            Dictionary with counts and sample discrepancies
        """
        logger.info(f"Processing {len(source_data)} source and {len(target_data)} target rows in batches...")

        source_index = self.build_key_index(source_data, key_field)
        target_index = self.build_key_index(target_data, key_field)

        missing_count = 0
        extra_count = 0
        mismatch_count = 0

        # Process in batches
        source_keys = list(source_index.keys())
        target_keys = list(target_index.keys())

        for i in range(0, max(len(source_keys), len(target_keys)), batch_size):
            batch_source_keys = set(source_keys[i:i+batch_size])
            batch_target_keys = set(target_keys[i:i+batch_size])

            # Missing in this batch
            batch_missing = batch_source_keys - set(target_index.keys())
            missing_count += len(batch_missing)

            # Extra in this batch
            batch_extra = batch_target_keys - set(source_index.keys())
            extra_count += len(batch_extra)

            # Mismatches in this batch
            batch_common = batch_source_keys & batch_target_keys
            for key in batch_common:
                if key in source_index and key in target_index:
                    if not self.comparer.compare_rows(
                        source_index[key],
                        target_index[key],
                        ignore_fields=ignore_fields
                    ):
                        mismatch_count += 1

        logger.info(f"Batch processing complete: {missing_count} missing, {extra_count} extra, {mismatch_count} mismatched")

        return {
            "missing_count": missing_count,
            "extra_count": extra_count,
            "mismatch_count": mismatch_count
        }

    def get_discrepancy_summary(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        ignore_fields: Optional[List[str]] = None
    ) -> Dict[str, int]:
        """
        Get summary statistics of discrepancies.

        Args:
            source_data: Source dataset
            target_data: Target dataset
            key_field: Key field(s)
            ignore_fields: Fields to ignore

        Returns:
            Summary dictionary with counts
        """
        discrepancies = self.find_all_discrepancies(
            source_data,
            target_data,
            key_field,
            ignore_fields
        )

        source_index = self.build_key_index(source_data, key_field)
        target_index = self.build_key_index(target_data, key_field)
        common_keys = set(source_index.keys()) & set(target_index.keys())

        match_count = len(common_keys) - len(discrepancies["mismatches"])

        return {
            "total_source_rows": len(source_data),
            "total_target_rows": len(target_data),
            "missing_count": len(discrepancies["missing"]),
            "extra_count": len(discrepancies["extra"]),
            "mismatch_count": len(discrepancies["mismatches"]),
            "match_count": match_count
        }

    def find_duplicates(
        self,
        data: List[Dict[str, Any]],
        key_field: Union[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Find duplicate keys in a dataset.

        Args:
            data: Dataset to check
            key_field: Key field(s)

        Returns:
            List of duplicates with counts
        """
        key_counts = defaultdict(int)

        for row in data:
            key = self._extract_key(row, key_field)
            key_counts[key] += 1

        duplicates = [
            {"key": key, "count": count}
            for key, count in key_counts.items()
            if count > 1
        ]

        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate keys")

        return duplicates

    def build_key_index(
        self,
        data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        case_sensitive_keys: bool = True
    ) -> Dict[str, Dict[str, Any]]:
        """
        Build index for fast key lookups.

        Args:
            data: Dataset to index
            key_field: Field name(s) to use as key
            case_sensitive_keys: Whether keys are case-sensitive

        Returns:
            Dictionary mapping key → row

        Raises:
            KeyError: If key field is missing from any row
            ValueError: If key field contains NULL in any row
        """
        index = {}

        for i, row in enumerate(data):
            try:
                key = self._extract_key(row, key_field, case_sensitive_keys)
                index[key] = row
            except (KeyError, ValueError) as e:
                logger.error(
                    f"Failed to extract key from row {i}: {e}. "
                    f"Row data: {row}"
                )
                raise ValueError(
                    f"Invalid row at index {i}: {e}"
                ) from e

        return index

    def get_row_by_key(
        self,
        data: List[Dict[str, Any]],
        key_field: Union[str, List[str]],
        key_value: Union[str, List[str]]
    ) -> Optional[Dict[str, Any]]:
        """
        Get row by key value.

        Args:
            data: Dataset
            key_field: Key field(s)
            key_value: Key value(s) to find

        Returns:
            Row if found, None otherwise
        """
        if isinstance(key_field, list) and isinstance(key_value, list):
            target_key = tuple(str(v) for v in key_value)
        else:
            target_key = str(key_value)

        for row in data:
            row_key = self._extract_key(row, key_field)
            if row_key == target_key:
                return row

        return None

    def calculate_match_percentage(
        self,
        discrepancies: Dict[str, List],
        total_source_rows: int
    ) -> float:
        """
        Calculate percentage of matching rows.

        Args:
            discrepancies: Discrepancies dictionary
            total_source_rows: Total number of source rows

        Returns:
            Match percentage (0-100)
        """
        if total_source_rows == 0:
            return 100.0

        total_issues = (
            len(discrepancies["missing"]) +
            len(discrepancies["mismatches"])
        )

        matching_rows = total_source_rows - total_issues
        percentage = (matching_rows / total_source_rows) * 100.0

        return round(percentage, 2)

    def find_schema_differences(
        self,
        source_data: List[Dict[str, Any]],
        target_data: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """
        Find schema differences between datasets.

        Args:
            source_data: Source dataset
            target_data: Target dataset

        Returns:
            Dictionary with:
            - only_in_source: Fields only in source
            - only_in_target: Fields only in target
            - common_fields: Fields in both
        """
        if not source_data and not target_data:
            return {
                "only_in_source": [],
                "only_in_target": [],
                "common_fields": []
            }

        # Aggregate all fields from all rows (not just first row)
        source_fields = set()
        if source_data:
            for row in source_data:
                source_fields.update(row.keys())

        target_fields = set()
        if target_data:
            for row in target_data:
                target_fields.update(row.keys())

        return {
            "only_in_source": sorted(source_fields - target_fields),
            "only_in_target": sorted(target_fields - source_fields),
            "common_fields": sorted(source_fields & target_fields)
        }

    def _extract_key(
        self,
        row: Dict[str, Any],
        key_field: Union[str, List[str]],
        case_sensitive_keys: bool = True
    ) -> Union[str, Tuple[str, ...]]:
        """
        Extract key value(s) from a row.

        Args:
            row: Row dictionary
            key_field: Key field name(s)
            case_sensitive_keys: Whether keys are case-sensitive

        Returns:
            Key value (string for single key, tuple for composite key)

        Raises:
            KeyError: If key field is missing from row
            ValueError: If key field contains None value
        """
        if isinstance(key_field, list):
            # Composite key
            key_values = []
            for field in key_field:
                if field not in row:
                    raise KeyError(
                        f"Key field '{field}' not found in row. "
                        f"Available fields: {list(row.keys())}"
                    )

                value = row[field]
                if value is None:
                    raise ValueError(
                        f"Key field '{field}' has NULL value in row. "
                        f"Keys cannot be NULL. Row: {row}"
                    )

                str_value = str(value)
                if not case_sensitive_keys:
                    str_value = str_value.lower()
                key_values.append(str_value)
            return tuple(key_values)
        else:
            # Single key
            if key_field not in row:
                raise KeyError(
                    f"Key field '{key_field}' not found in row. "
                    f"Available fields: {list(row.keys())}"
                )

            value = row[key_field]
            if value is None:
                raise ValueError(
                    f"Key field '{key_field}' has NULL value in row. "
                    f"Keys cannot be NULL. Row: {row}"
                )

            str_value = str(value)
            if not case_sensitive_keys:
                str_value = str_value.lower()
            return str_value
