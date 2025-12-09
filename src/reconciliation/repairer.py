"""
Data Repairer for CDC Pipeline Reconciliation

Generates repair actions (INSERT/UPDATE/DELETE) based on detected discrepancies.
Produces SQL statements to fix inconsistencies between ScyllaDB and PostgreSQL.
"""

import logging
from typing import Dict, List, Any, Union, Optional
from datetime import datetime, timezone
import json

logger = logging.getLogger(__name__)


class DataRepairer:
    """
    Generates repair actions for discrepancies.

    Creates SQL statements to:
    - INSERT missing rows
    - DELETE extra rows
    - UPDATE mismatched rows
    """

    def __init__(self):
        """Initialize the data repairer."""
        logger.debug("Initialized DataRepairer")

    def generate_repair_actions(
        self,
        discrepancies: Dict[str, List[Dict[str, Any]]],
        table_name: str,
        schema: str,
        key_field: Union[str, List[str]],
        dry_run: bool = False,
        include_metadata: bool = True,
        prioritize: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Generate all repair actions from discrepancies.

        Args:
            discrepancies: Dictionary with missing, extra, mismatches
            table_name: Target table name
            schema: Target schema name
            key_field: Key field(s)
            dry_run: If True, mark actions as dry-run
            include_metadata: Include generation metadata
            prioritize: Order actions (DELETE → INSERT → UPDATE)

        Returns:
            List of repair actions
        """
        actions = []

        # Generate DELETE actions (do first to avoid conflicts)
        delete_actions = self.generate_delete_actions(
            discrepancies.get("extra", []),
            table_name=table_name,
            schema=schema,
            key_field=key_field
        )
        actions.extend(delete_actions)

        # Generate INSERT actions
        insert_actions = self.generate_insert_actions(
            discrepancies.get("missing", []),
            table_name=table_name,
            schema=schema
        )
        actions.extend(insert_actions)

        # Generate UPDATE actions
        update_actions = self.generate_update_actions(
            discrepancies.get("mismatches", []),
            table_name=table_name,
            schema=schema,
            key_field=key_field
        )
        actions.extend(update_actions)

        # Add metadata and dry-run flags
        timestamp = datetime.now(timezone.utc).isoformat()
        for action in actions:
            action["dry_run"] = dry_run
            action["status"] = "pending"

            if include_metadata:
                action["generated_at"] = timestamp
                # Determine discrepancy type
                if action["action_type"] == "INSERT":
                    action["discrepancy_type"] = "missing"
                elif action["action_type"] == "DELETE":
                    action["discrepancy_type"] = "extra"
                elif action["action_type"] == "UPDATE":
                    action["discrepancy_type"] = "mismatch"

        logger.info(
            f"Generated {len(actions)} repair actions: "
            f"{len(insert_actions)} INSERT, {len(delete_actions)} DELETE, "
            f"{len(update_actions)} UPDATE"
        )

        return actions

    def generate_insert_actions(
        self,
        missing_rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
        batch_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Generate INSERT actions for missing rows.

        Args:
            missing_rows: Rows to insert
            table_name: Target table
            schema: Target schema
            batch_size: If set, create batch inserts

        Returns:
            List of INSERT actions
        """
        if not missing_rows:
            return []

        if batch_size and batch_size > 1:
            # Generate batch inserts
            actions = []
            for i in range(0, len(missing_rows), batch_size):
                batch = missing_rows[i:i+batch_size]
                action = self._generate_batch_insert_sql(
                    batch,
                    table_name=table_name,
                    schema=schema
                )
                actions.append(action)
            return actions
        else:
            # Generate individual inserts
            return [
                self.generate_insert_sql(row, table_name, schema)
                for row in missing_rows
            ]

    def generate_delete_actions(
        self,
        extra_rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
        key_field: Union[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Generate DELETE actions for extra rows.

        Args:
            extra_rows: Rows to delete
            table_name: Target table
            schema: Target schema
            key_field: Key field(s)

        Returns:
            List of DELETE actions
        """
        if not extra_rows:
            return []

        return [
            self.generate_delete_sql(row, table_name, schema, key_field)
            for row in extra_rows
        ]

    def generate_update_actions(
        self,
        mismatched_rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
        key_field: Union[str, List[str]]
    ) -> List[Dict[str, Any]]:
        """
        Generate UPDATE actions for mismatched rows.

        Args:
            mismatched_rows: Mismatch records
            table_name: Target table
            schema: Target schema
            key_field: Key field(s)

        Returns:
            List of UPDATE actions
        """
        if not mismatched_rows:
            return []

        return [
            self.generate_update_sql(mismatch, table_name, schema, key_field)
            for mismatch in mismatched_rows
        ]

    def generate_insert_sql(
        self,
        row: Dict[str, Any],
        table_name: str,
        schema: str,
        quote_identifiers: bool = False
    ) -> Dict[str, Any]:
        """
        Generate INSERT SQL for a single row.

        Args:
            row: Row data
            table_name: Table name
            schema: Schema name
            quote_identifiers: Whether to quote identifiers

        Returns:
            Action dictionary with SQL
        """
        table_ref = self._format_table_name(schema, table_name, quote_identifiers)

        fields = list(row.keys())
        values = [self._format_value(row[field]) for field in fields]

        fields_str = ", ".join(fields)
        values_str = ", ".join(values)

        sql = f"INSERT INTO {table_ref} ({fields_str}) VALUES ({values_str});"

        return {
            "action_type": "INSERT",
            "table": f"{schema}.{table_name}",
            "sql": sql,
            "row_data": row,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def generate_delete_sql(
        self,
        row: Dict[str, Any],
        table_name: str,
        schema: str,
        key_field: Union[str, List[str]],
        quote_identifiers: bool = False
    ) -> Dict[str, Any]:
        """
        Generate DELETE SQL for a row.

        Args:
            row: Row to delete
            table_name: Table name
            schema: Schema name
            key_field: Key field(s)
            quote_identifiers: Quote identifiers

        Returns:
            Action dictionary with SQL
        """
        table_ref = self._format_table_name(schema, table_name, quote_identifiers)

        where_clause = self._build_where_clause(row, key_field)

        sql = f"DELETE FROM {table_ref} WHERE {where_clause};"

        return {
            "action_type": "DELETE",
            "table": f"{schema}.{table_name}",
            "sql": sql,
            "row_data": row,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def generate_update_sql(
        self,
        mismatch: Dict[str, Any],
        table_name: str,
        schema: str,
        key_field: Union[str, List[str]],
        quote_identifiers: bool = False
    ) -> Dict[str, Any]:
        """
        Generate UPDATE SQL for a mismatch.

        Args:
            mismatch: Mismatch record with scylla and postgres rows
            table_name: Table name
            schema: Schema name
            key_field: Key field(s)
            quote_identifiers: Quote identifiers

        Returns:
            Action dictionary with SQL
        """
        table_ref = self._format_table_name(schema, table_name, quote_identifiers)

        scylla_row = mismatch["scylla"]
        postgres_row = mismatch["postgres"]

        # Determine which fields to update (fields that differ)
        update_fields = []
        for field in scylla_row:
            if field in postgres_row and scylla_row[field] != postgres_row[field]:
                update_fields.append(field)

        # If no specific differing fields detected, update all non-key fields
        if not update_fields:
            key_fields = [key_field] if isinstance(key_field, str) else key_field
            update_fields = [f for f in scylla_row.keys() if f not in key_fields]

        # Build SET clause
        set_parts = []
        for field in update_fields:
            value = self._format_value(scylla_row[field])
            set_parts.append(f"{field} = {value}")

        set_clause = ", ".join(set_parts)

        # Build WHERE clause
        where_clause = self._build_where_clause(scylla_row, key_field)

        sql = f"UPDATE {table_ref} SET {set_clause} WHERE {where_clause};"

        return {
            "action_type": "UPDATE",
            "table": f"{schema}.{table_name}",
            "sql": sql,
            "row_data": scylla_row,
            "updated_fields": update_fields,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _generate_batch_insert_sql(
        self,
        rows: List[Dict[str, Any]],
        table_name: str,
        schema: str,
        quote_identifiers: bool = False
    ) -> Dict[str, Any]:
        """
        Generate batch INSERT SQL.

        Args:
            rows: List of rows
            table_name: Table name
            schema: Schema name
            quote_identifiers: Quote identifiers

        Returns:
            Action dictionary with batch INSERT SQL
        """
        if not rows:
            raise ValueError("Cannot generate batch insert for empty rows")

        table_ref = self._format_table_name(schema, table_name, quote_identifiers)

        # Use fields from first row
        fields = list(rows[0].keys())
        fields_str = ", ".join(fields)

        # Generate values for each row
        values_list = []
        for row in rows:
            values = [self._format_value(row.get(field)) for field in fields]
            values_str = ", ".join(values)
            values_list.append(f"({values_str})")

        all_values = ",\n    ".join(values_list)

        sql = f"INSERT INTO {table_ref} ({fields_str}) VALUES\n    {all_values};"

        return {
            "action_type": "INSERT",
            "table": f"{schema}.{table_name}",
            "sql": sql,
            "row_data": rows,
            "batch_size": len(rows),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _build_where_clause(
        self,
        row: Dict[str, Any],
        key_field: Union[str, List[str]]
    ) -> str:
        """
        Build WHERE clause for DELETE/UPDATE.

        Args:
            row: Row data
            key_field: Key field(s)

        Returns:
            WHERE clause string
        """
        if isinstance(key_field, list):
            # Composite key
            conditions = []
            for field in key_field:
                value = self._format_value(row[field])
                conditions.append(f"{field} = {value}")
            return " AND ".join(conditions)
        else:
            # Single key
            value = self._format_value(row[key_field])
            return f"{key_field} = {value}"

    def _format_table_name(
        self,
        schema: str,
        table_name: str,
        quote_identifiers: bool = False
    ) -> str:
        """
        Format table name with schema.

        Args:
            schema: Schema name
            table_name: Table name
            quote_identifiers: Whether to quote identifiers

        Returns:
            Formatted table reference
        """
        if quote_identifiers:
            return f'"{schema}"."{table_name}"'
        else:
            return f"{schema}.{table_name}"

    def _format_value(self, value: Any) -> str:
        """
        Format a value for SQL.

        Args:
            value: Value to format

        Returns:
            SQL-formatted value string
        """
        if value is None:
            return "NULL"

        if isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            return f"'{escaped}'"

        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, datetime):
            return f"'{value.isoformat()}'"

        if isinstance(value, (list, dict)):
            # Convert to JSON string
            json_str = json.dumps(value).replace("'", "''")
            return f"'{json_str}'"

        # Default: convert to string
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
