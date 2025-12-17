# Bug Fix Implementation Plan
## ScyllaDB PostgreSQL CDC Pipeline

**Plan Version:** 1.0
**Created:** 2025-12-17
**Based on:** SWOT Analysis v0.0
**Total Bugs:** 8 (0 Critical, 3 High, 3 Medium, 2 Low)
**Estimated Total Effort:** 5-7 days

---

## Table of Contents

1. [Overview](#overview)
2. [Sprint Planning](#sprint-planning)
3. [High Priority Bugs](#high-priority-bugs)
4. [Medium Priority Bugs](#medium-priority-bugs)
5. [Low Priority Bugs](#low-priority-bugs)
6. [Testing Strategy](#testing-strategy)
7. [Rollout Plan](#rollout-plan)
8. [Success Metrics](#success-metrics)

---

## Overview

This document outlines the implementation plan for fixing all 8 bugs identified in the SWOT analysis. The bugs are organized by priority and include detailed steps, test requirements, and acceptance criteria.

### Priority Distribution

| Priority | Count | Estimated Effort | Target Completion |
|----------|-------|------------------|-------------------|
| High | 3 | 2-3 days | Week 1 (2025-12-24) |
| Medium | 3 | 2-3 days | Week 2 (2025-12-31) |
| Low | 2 | 1 day | Week 3 (2026-01-07) |

### Risk Assessment

- **Low Risk:** Bugs #4, #6, #7, #8 (internal improvements)
- **Medium Risk:** Bugs #2, #3, #5 (data handling improvements)
- **High Risk:** Bug #1 (SQL generation changes - requires careful testing)

---

## Sprint Planning

### Sprint 1: High Priority Bugs (Week of 2025-12-18)

**Goal:** Fix SQL injection vulnerability and core data handling issues

**Bugs to Address:**
- Bug #1: Unsafe Field Name Handling in SQL Generation
- Bug #2: Missing Key Validation in Differ
- Bug #3: Unhandled Data Types in SQL Formatter

**Team Capacity:** 1 developer, 5 days
**Buffer:** 20% for testing and code review

---

### Sprint 2: Medium Priority Bugs (Week of 2025-12-25)

**Goal:** Fix comparison logic and batch processing

**Bugs to Address:**
- Bug #4: Float Tolerance Side Effect
- Bug #5: Incomplete Batch Processing
- Bug #6: Schema Validation Missing Required Fields

**Team Capacity:** 1 developer, 5 days
**Buffer:** 20% for testing and integration

---

### Sprint 3: Low Priority Bugs (Week of 2026-01-01)

**Goal:** Polish edge cases and improve observability

**Bugs to Address:**
- Bug #7: Health Check Return Value Inconsistency
- Bug #8: Missing Test for Empty Dataset Edge Case

**Team Capacity:** 1 developer, 2 days
**Buffer:** 20% for documentation updates

---

## High Priority Bugs

### Bug #1: Unsafe Field Name Handling in SQL Generation

**Priority:** HIGH
**Severity:** Medium-High
**Effort:** 1.5 days
**Files Affected:**
- `src/reconciliation/repairer.py`
- `tests/unit/test_repairer.py`

#### Problem Statement

Field names in INSERT/UPDATE/DELETE statements are not properly escaped, allowing SQL keywords or special characters to break queries or create potential SQL injection vulnerabilities.

```python
# Current (UNSAFE):
fields_str = ", ".join(fields)
sql = f"INSERT INTO {table_ref} ({fields_str}) VALUES ({values_str});"

# Problem: If fields = ["id", "order", "total"]
# Generates: INSERT INTO table (id, order, total) VALUES ...
# PostgreSQL fails because "order" is a keyword
```

#### Root Cause

The `generate_insert_sql()`, `generate_update_sql()`, and `generate_delete_sql()` methods construct SQL strings without quoting identifiers.

#### Implementation Steps

**Step 1: Create Identifier Quoting Utility (30 min)**

```python
# Add to src/reconciliation/repairer.py

def _quote_identifier(self, identifier: str) -> str:
    """
    Quote a SQL identifier for safe use in queries.

    Args:
        identifier: Column name, table name, or schema name

    Returns:
        Properly quoted identifier

    Examples:
        >>> _quote_identifier("order")
        '"order"'
        >>> _quote_identifier("user_id")
        '"user_id"'
        >>> _quote_identifier("my-table")
        '"my-table"'
    """
    # Escape double quotes by doubling them
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'

def _quote_identifier_list(self, identifiers: List[str]) -> str:
    """
    Quote a list of identifiers and join with commas.

    Args:
        identifiers: List of column names

    Returns:
        Comma-separated quoted identifiers

    Examples:
        >>> _quote_identifier_list(["id", "order", "total"])
        '"id", "order", "total"'
    """
    return ", ".join(self._quote_identifier(i) for i in identifiers)
```

**Step 2: Update INSERT Statement Generation (45 min)**

```python
# Update src/reconciliation/repairer.py:202-237

def generate_insert_sql(
    self,
    row: Dict[str, Any],
    table_name: str,
    schema: str,
    quote_identifiers: bool = True  # Changed default to True
) -> Dict[str, Any]:
    """
    Generate INSERT SQL for a single row.

    Args:
        row: Row data
        table_name: Table name
        schema: Schema name
        quote_identifiers: Whether to quote identifiers (default: True)

    Returns:
        Action dictionary with SQL
    """
    table_ref = self._format_table_name(schema, table_name, quote_identifiers=True)

    fields = list(row.keys())
    values = [self._format_value(row[field]) for field in fields]

    # Quote all field names
    if quote_identifiers:
        fields_str = self._quote_identifier_list(fields)
    else:
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
```

**Step 3: Update UPDATE Statement Generation (45 min)**

```python
# Update src/reconciliation/repairer.py:274-331

def generate_update_sql(
    self,
    mismatch: Dict[str, Any],
    table_name: str,
    schema: str,
    key_field: Union[str, List[str]],
    quote_identifiers: bool = True  # Changed default to True
) -> Dict[str, Any]:
    """
    Generate UPDATE SQL for a mismatch.
    """
    table_ref = self._format_table_name(schema, table_name, quote_identifiers=True)

    scylla_row = mismatch["scylla"]
    postgres_row = mismatch["postgres"]

    # Determine which fields to update
    update_fields = []
    for field in scylla_row:
        if field in postgres_row and scylla_row[field] != postgres_row[field]:
            update_fields.append(field)

    if not update_fields:
        key_fields = [key_field] if isinstance(key_field, str) else key_field
        update_fields = [f for f in scylla_row.keys() if f not in key_fields]

    # Build SET clause with quoted identifiers
    set_parts = []
    for field in update_fields:
        value = self._format_value(scylla_row[field])
        quoted_field = self._quote_identifier(field) if quote_identifiers else field
        set_parts.append(f"{quoted_field} = {value}")

    set_clause = ", ".join(set_parts)

    # Build WHERE clause with quoted identifiers
    where_clause = self._build_where_clause(scylla_row, key_field, quote_identifiers)

    sql = f"UPDATE {table_ref} SET {set_clause} WHERE {where_clause};"

    return {
        "action_type": "UPDATE",
        "table": f"{schema}.{table_name}",
        "sql": sql,
        "row_data": scylla_row,
        "updated_fields": update_fields,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
```

**Step 4: Update DELETE Statement Generation (30 min)**

```python
# Update src/reconciliation/repairer.py:239-272

def generate_delete_sql(
    self,
    row: Dict[str, Any],
    table_name: str,
    schema: str,
    key_field: Union[str, List[str]],
    quote_identifiers: bool = True  # Changed default to True
) -> Dict[str, Any]:
    """
    Generate DELETE SQL for a row.
    """
    table_ref = self._format_table_name(schema, table_name, quote_identifiers=True)

    where_clause = self._build_where_clause(row, key_field, quote_identifiers)

    sql = f"DELETE FROM {table_ref} WHERE {where_clause};"

    return {
        "action_type": "DELETE",
        "table": f"{schema}.{table_name}",
        "sql": sql,
        "row_data": row,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
```

**Step 5: Update WHERE Clause Builder (30 min)**

```python
# Update src/reconciliation/repairer.py:381-406

def _build_where_clause(
    self,
    row: Dict[str, Any],
    key_field: Union[str, List[str]],
    quote_identifiers: bool = True  # Add parameter
) -> str:
    """
    Build WHERE clause for DELETE/UPDATE.

    Args:
        row: Row data
        key_field: Key field(s)
        quote_identifiers: Whether to quote identifiers

    Returns:
        WHERE clause string
    """
    if isinstance(key_field, list):
        # Composite key
        conditions = []
        for field in key_field:
            value = self._format_value(row[field])
            quoted_field = self._quote_identifier(field) if quote_identifiers else field
            conditions.append(f"{quoted_field} = {value}")
        return " AND ".join(conditions)
    else:
        # Single key
        value = self._format_value(row[key_field])
        quoted_field = self._quote_identifier(key_field) if quote_identifiers else key_field
        return f"{quoted_field} = {value}"
```

**Step 6: Update Batch INSERT (30 min)**

```python
# Update src/reconciliation/repairer.py:333-379

def _generate_batch_insert_sql(
    self,
    rows: List[Dict[str, Any]],
    table_name: str,
    schema: str,
    quote_identifiers: bool = True  # Changed default to True
) -> Dict[str, Any]:
    """
    Generate batch INSERT SQL.
    """
    if not rows:
        raise ValueError("Cannot generate batch insert for empty rows")

    table_ref = self._format_table_name(schema, table_name, quote_identifiers=True)

    # Use fields from first row
    fields = list(rows[0].keys())

    # Quote field names
    if quote_identifiers:
        fields_str = self._quote_identifier_list(fields)
    else:
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
```

**Step 7: Add Comprehensive Tests (3 hours)**

```python
# Add to tests/unit/test_repairer.py

def test_insert_sql_with_reserved_keywords():
    """Test INSERT with SQL reserved keywords as field names."""
    repairer = DataRepairer()

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
    expected = 'INSERT INTO "test_schema"."test_table" ("id", "order", "select", "from") VALUES'
    assert expected in result["sql"]


def test_update_sql_with_special_characters():
    """Test UPDATE with special characters in field names."""
    repairer = DataRepairer()

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


def test_delete_sql_with_composite_key():
    """Test DELETE with composite key containing reserved words."""
    repairer = DataRepairer()

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
    assert '"table" =' in result["sql"]
    assert '"order" =' in result["sql"]
    assert 'WHERE "table" = ' in result["sql"]


def test_quote_identifier_escapes_double_quotes():
    """Test that double quotes in identifiers are escaped."""
    repairer = DataRepairer()

    # Field name with double quote
    quoted = repairer._quote_identifier('field"name')
    assert quoted == '"field""name"'  # Double quotes are doubled


def test_sql_injection_prevention():
    """Test that SQL injection attempts are neutralized."""
    repairer = DataRepairer()

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


def test_backward_compatibility_quote_identifiers_false():
    """Test backward compatibility when quote_identifiers=False."""
    repairer = DataRepairer()

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
    assert 'INSERT INTO "test_schema"."test_table" (id, name)' in result["sql"]
```

#### Testing Checklist

- [ ] Unit tests pass with reserved keywords (order, select, from, table, etc.)
- [ ] Unit tests pass with special characters (hyphens, @, spaces)
- [ ] Unit tests pass with double quotes in identifiers
- [ ] Unit tests pass with SQL injection attempts
- [ ] Integration tests with actual PostgreSQL database
- [ ] Backward compatibility tests with quote_identifiers=False
- [ ] Performance tests (quoting should add <1% overhead)

#### Acceptance Criteria

- [ ] All SQL statements quote field names by default
- [ ] Double quotes in identifiers are properly escaped
- [ ] SQL injection attempts are neutralized
- [ ] All existing tests still pass
- [ ] New tests cover edge cases
- [ ] Code review approved by 2+ reviewers
- [ ] Documentation updated with examples

#### Rollback Plan

If issues are detected in production:
1. Revert to previous version using git
2. Set `quote_identifiers=False` globally as temporary fix
3. Monitor for query failures
4. Re-deploy after additional testing

---

### Bug #2: Missing Key Validation in Differ

**Priority:** HIGH
**Severity:** Medium
**Effort:** 0.5 days
**Files Affected:**
- `src/reconciliation/differ.py`
- `tests/unit/test_differ.py`

#### Problem Statement

The `_extract_key()` method doesn't validate that key fields exist in rows before extraction, silently converting missing keys to empty strings.

```python
# Current (UNSAFE):
value = row.get(field)  # Returns None if missing
if value is None:
    value = ""  # Could mask missing keys!
```

#### Implementation Steps

**Step 1: Add Key Validation (30 min)**

```python
# Update src/reconciliation/differ.py:469-506

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
```

**Step 2: Add Graceful Error Handling (30 min)**

```python
# Update src/reconciliation/differ.py:353-376

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
```

**Step 3: Add Comprehensive Tests (1 hour)**

```python
# Add to tests/unit/test_differ.py

def test_extract_key_missing_field_raises_error():
    """Test that missing key field raises KeyError."""
    differ = DataDiffer()

    row = {"name": "John", "email": "john@example.com"}

    with pytest.raises(KeyError) as exc_info:
        differ._extract_key(row, "user_id")

    assert "Key field 'user_id' not found" in str(exc_info.value)
    assert "Available fields: ['name', 'email']" in str(exc_info.value)


def test_extract_key_null_value_raises_error():
    """Test that NULL key value raises ValueError."""
    differ = DataDiffer()

    row = {"user_id": None, "name": "John"}

    with pytest.raises(ValueError) as exc_info:
        differ._extract_key(row, "user_id")

    assert "Key field 'user_id' has NULL value" in str(exc_info.value)
    assert "Keys cannot be NULL" in str(exc_info.value)


def test_extract_composite_key_missing_field():
    """Test composite key with missing field."""
    differ = DataDiffer()

    row = {"order_id": 123, "customer_id": 456}

    with pytest.raises(KeyError) as exc_info:
        differ._extract_key(row, ["order_id", "item_id"])

    assert "Key field 'item_id' not found" in str(exc_info.value)


def test_extract_composite_key_null_value():
    """Test composite key with NULL value."""
    differ = DataDiffer()

    row = {"order_id": 123, "item_id": None}

    with pytest.raises(ValueError) as exc_info:
        differ._extract_key(row, ["order_id", "item_id"])

    assert "Key field 'item_id' has NULL value" in str(exc_info.value)


def test_build_key_index_with_invalid_row():
    """Test that build_key_index fails fast on invalid row."""
    differ = DataDiffer()

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


def test_find_discrepancies_with_missing_keys():
    """Test discrepancy detection handles missing keys gracefully."""
    differ = DataDiffer()

    source = [
        {"id": 1, "value": "A"},
        {"id": 2, "value": "B"}
    ]

    target = [
        {"value": "A"},  # Missing 'id'
        {"id": 2, "value": "B"}
    ]

    with pytest.raises(ValueError):
        differ.find_all_discrepancies(source, target, "id")
```

#### Acceptance Criteria

- [ ] Missing key fields raise `KeyError` with helpful message
- [ ] NULL key values raise `ValueError` with helpful message
- [ ] Error messages include available fields and row data
- [ ] Composite keys validate all fields
- [ ] All existing tests still pass
- [ ] New tests cover edge cases

---

### Bug #3: Unhandled Data Types in SQL Formatter

**Priority:** HIGH
**Severity:** Medium
**Effort:** 1 day
**Files Affected:**
- `src/reconciliation/repairer.py`
- `tests/unit/test_repairer.py`

#### Problem Statement

The `_format_value()` method doesn't handle several important data types: bytes, UUID, timedelta, and custom objects.

#### Implementation Steps

**Step 1: Add Support for Additional Types (2 hours)**

```python
# Update src/reconciliation/repairer.py:430-464

from uuid import UUID
from datetime import datetime, timezone, timedelta
from decimal import Decimal

def _format_value(self, value: Any) -> str:
    """
    Format a value for SQL.

    Supports:
        - None → NULL
        - str → 'escaped string'
        - bool → TRUE/FALSE
        - int, float → numeric literal
        - Decimal → numeric literal
        - datetime → ISO 8601 timestamp
        - timedelta → PostgreSQL interval
        - UUID → UUID literal
        - bytes → bytea hex format
        - list, dict → JSON

    Args:
        value: Value to format

    Returns:
        SQL-formatted value string

    Raises:
        TypeError: If value type is not supported
    """
    if value is None:
        return "NULL"

    # String types
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    # Boolean (must come before int, as bool is subclass of int)
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    # Numeric types
    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, Decimal):
        return str(value)

    # UUID type
    if isinstance(value, UUID):
        return f"'{str(value)}'"

    # Datetime types
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    if isinstance(value, timedelta):
        # Convert to PostgreSQL interval format
        total_seconds = int(value.total_seconds())
        return f"INTERVAL '{total_seconds} seconds'"

    # Binary data
    if isinstance(value, (bytes, bytearray)):
        # PostgreSQL bytea hex format: '\xDEADBEEF'
        hex_string = value.hex()
        return f"'\\x{hex_string}'"

    # Collections (JSON)
    if isinstance(value, (list, dict)):
        json_str = json.dumps(value).replace("'", "''")
        return f"'{json_str}'"

    # Unsupported type
    raise TypeError(
        f"Unsupported data type for SQL formatting: {type(value).__name__}. "
        f"Value: {value}. "
        f"Supported types: None, str, bool, int, float, Decimal, datetime, "
        f"timedelta, UUID, bytes, bytearray, list, dict"
    )
```

**Step 2: Add Comprehensive Tests (3 hours)**

```python
# Add to tests/unit/test_repairer.py

def test_format_value_uuid():
    """Test UUID formatting."""
    repairer = DataRepairer()
    uuid_val = UUID('123e4567-e89b-12d3-a456-426614174000')

    result = repairer._format_value(uuid_val)
    assert result == "'123e4567-e89b-12d3-a456-426614174000'"


def test_format_value_bytes():
    """Test bytes formatting for PostgreSQL bytea."""
    repairer = DataRepairer()

    # Binary data
    binary_data = b'\x00\x01\x02\xDE\xAD\xBE\xEF'
    result = repairer._format_value(binary_data)

    assert result == "'\\x000102deadbeef'"


def test_format_value_bytearray():
    """Test bytearray formatting."""
    repairer = DataRepairer()

    data = bytearray([0, 1, 2, 255])
    result = repairer._format_value(data)

    assert result == "'\\x000102ff'"


def test_format_value_timedelta():
    """Test timedelta formatting as PostgreSQL interval."""
    repairer = DataRepairer()

    # 1 hour, 30 minutes
    delta = timedelta(hours=1, minutes=30)
    result = repairer._format_value(delta)

    assert result == "INTERVAL '5400 seconds'"


def test_format_value_decimal():
    """Test Decimal formatting."""
    repairer = DataRepairer()

    decimal_val = Decimal('123.456')
    result = repairer._format_value(decimal_val)

    assert result == "123.456"


def test_format_value_large_number():
    """Test very large number formatting."""
    repairer = DataRepairer()

    large_num = 9999999999999999999999999999
    result = repairer._format_value(large_num)

    assert result == "9999999999999999999999999999"


def test_format_value_unsupported_type_raises_error():
    """Test that unsupported types raise TypeError."""
    repairer = DataRepairer()

    # Custom class
    class CustomObject:
        pass

    obj = CustomObject()

    with pytest.raises(TypeError) as exc_info:
        repairer._format_value(obj)

    assert "Unsupported data type" in str(exc_info.value)
    assert "CustomObject" in str(exc_info.value)
    assert "Supported types:" in str(exc_info.value)


def test_insert_with_binary_data():
    """Integration test: INSERT with binary data."""
    repairer = DataRepairer()

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


def test_update_with_timedelta():
    """Integration test: UPDATE with timedelta."""
    repairer = DataRepairer()

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
```

#### Acceptance Criteria

- [ ] UUID values are formatted correctly
- [ ] bytes/bytearray use PostgreSQL bytea hex format
- [ ] timedelta uses PostgreSQL INTERVAL format
- [ ] Decimal numbers preserve precision
- [ ] Unsupported types raise TypeError with helpful message
- [ ] All existing tests still pass
- [ ] Integration tests with real PostgreSQL database

---

## Medium Priority Bugs

### Bug #4: Float Tolerance Side Effect

**Priority:** MEDIUM
**Effort:** 0.5 days
**Files Affected:**
- `src/reconciliation/comparer.py`
- `tests/unit/test_comparer.py`

#### Implementation Steps

**Step 1: Fix Instance Variable Mutation (15 min)**

```python
# Update src/reconciliation/comparer.py:30-94

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
        float_tolerance: Tolerance for floating point comparison (per-call basis)

    Returns:
        True if rows are equal, False otherwise
    """
    # Use local variable instead of modifying instance state
    tolerance = float_tolerance if float_tolerance is not None else self.float_tolerance

    # Normalize both rows
    norm_scylla = self.normalize_row(scylla_row)
    norm_postgres = self.normalize_row(postgres_row)

    # ... rest of method unchanged ...

    # Compare all common fields
    for scylla_field, postgres_field in common_fields:
        scylla_value = norm_scylla[scylla_field]
        postgres_value = norm_postgres[postgres_field]

        # Pass tolerance to comparison method
        if not self._values_equal(scylla_value, postgres_value, tolerance):
            logger.debug(
                f"Field {scylla_field} mismatch: "
                f"ScyllaDB={scylla_value}, PostgreSQL={postgres_value}"
            )
            return False

    return True
```

**Step 2: Update Value Comparison (15 min)**

```python
# Update src/reconciliation/comparer.py:231-288

def _values_equal(self, value1: Any, value2: Any, float_tolerance: float = None) -> bool:
    """
    Compare two normalized values for equality.

    Args:
        value1: First value
        value2: Second value
        float_tolerance: Tolerance for float comparison (overrides instance default)

    Returns:
        True if values are equal
    """
    # Use provided tolerance or fall back to instance default
    tolerance = float_tolerance if float_tolerance is not None else self.float_tolerance

    # ... existing None checks ...

    # Handle float comparison with tolerance
    if isinstance(value1, float) and isinstance(value2, float):
        return abs(value1 - value2) < tolerance

    # ... rest of method unchanged ...
```

**Step 3: Add Tests (30 min)**

```python
# Add to tests/unit/test_comparer.py

def test_float_tolerance_no_side_effects():
    """Test that float_tolerance parameter doesn't affect instance state."""
    comparer = RowComparer()

    # Default tolerance is 0.0001
    assert comparer.float_tolerance == 0.0001

    row1 = {"id": 1, "value": 1.001}
    row2 = {"id": 1, "value": 1.002}

    # Use custom tolerance for one comparison
    result1 = comparer.compare_rows(row1, row2, float_tolerance=0.01)
    assert result1 is True

    # Instance tolerance should be unchanged
    assert comparer.float_tolerance == 0.0001

    # Next comparison should use default tolerance
    result2 = comparer.compare_rows(row1, row2)
    assert result2 is False  # Difference is > 0.0001


def test_float_tolerance_thread_safety():
    """Test float tolerance in concurrent scenarios."""
    import threading

    comparer = RowComparer()
    results = []

    def compare_with_tolerance(tolerance, expected):
        row1 = {"value": 1.0}
        row2 = {"value": 1.001}
        result = comparer.compare_rows(row1, row2, float_tolerance=tolerance)
        results.append((result, expected))

    # Run comparisons concurrently
    threads = [
        threading.Thread(target=compare_with_tolerance, args=(0.01, True)),
        threading.Thread(target=compare_with_tolerance, args=(0.0001, False))
    ]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Verify each comparison got correct result
    for result, expected in results:
        assert result == expected
```

---

### Bug #5: Incomplete Batch Processing

**Priority:** MEDIUM
**Effort:** 1.5 days
**Files Affected:**
- `src/reconciliation/differ.py`
- `tests/unit/test_differ.py`

#### Implementation Steps

**Step 1: Implement True Streaming Comparison (4 hours)**

```python
# Add to src/reconciliation/differ.py

def find_all_discrepancies_streaming(
    self,
    source_data: List[Dict[str, Any]],
    target_data: List[Dict[str, Any]],
    key_field: Union[str, List[str]],
    batch_size: int = 1000,
    ignore_fields: Optional[List[str]] = None,
    progress_callback: Optional[Callable[[int, int], None]] = None
) -> Dict[str, Any]:
    """
    Find discrepancies using streaming/batched approach for memory efficiency.

    This method doesn't load full datasets into memory. Instead, it:
    1. Builds indices in batches
    2. Compares in batches
    3. Yields results incrementally

    Args:
        source_data: Source dataset
        target_data: Target dataset
        key_field: Key field(s)
        batch_size: Rows per batch
        ignore_fields: Fields to ignore
        progress_callback: Optional callback(current, total)

    Returns:
        Dictionary with discrepancy counts and samples
    """
    logger.info(f"Streaming comparison of {len(source_data)} source and {len(target_data)} target rows")

    # Results accumulators
    missing_count = 0
    extra_count = 0
    mismatch_count = 0

    missing_samples = []
    extra_samples = []
    mismatch_samples = []

    MAX_SAMPLES = 100

    # Build target index in batches (for lookups)
    target_keys = set()
    target_batches = []

    for i in range(0, len(target_data), batch_size):
        batch = target_data[i:i+batch_size]
        batch_index = self.build_key_index(batch, key_field)
        target_batches.append(batch_index)
        target_keys.update(batch_index.keys())

        if progress_callback:
            progress_callback(i, len(target_data))

    # Process source data in batches
    for i in range(0, len(source_data), batch_size):
        batch = source_data[i:i+batch_size]

        for row in batch:
            key = self._extract_key(row, key_field)

            # Check if key exists in target
            if key not in target_keys:
                missing_count += 1
                if len(missing_samples) < MAX_SAMPLES:
                    missing_samples.append(row)
            else:
                # Find target row across batches
                target_row = None
                for target_batch in target_batches:
                    if key in target_batch:
                        target_row = target_batch[key]
                        break

                # Compare values
                if target_row and not self.comparer.compare_rows(row, target_row, ignore_fields=ignore_fields):
                    mismatch_count += 1
                    if len(mismatch_samples) < MAX_SAMPLES:
                        mismatch_samples.append({
                            "key": key,
                            "scylla": row,
                            "postgres": target_row
                        })

        if progress_callback:
            progress_callback(len(source_data) + i, len(source_data) + len(target_data))

    # Find extra rows (in target but not source)
    source_keys = set()
    for i in range(0, len(source_data), batch_size):
        batch = source_data[i:i+batch_size]
        batch_index = self.build_key_index(batch, key_field)
        source_keys.update(batch_index.keys())

    for target_batch in target_batches:
        for key, row in target_batch.items():
            if key not in source_keys:
                extra_count += 1
                if len(extra_samples) < MAX_SAMPLES:
                    extra_samples.append(row)

    logger.info(
        f"Streaming comparison complete: {missing_count} missing, "
        f"{extra_count} extra, {mismatch_count} mismatched"
    )

    return {
        "missing_count": missing_count,
        "extra_count": extra_count,
        "mismatch_count": mismatch_count,
        "missing_samples": missing_samples,
        "extra_samples": extra_samples,
        "mismatch_samples": mismatch_samples,
        "total_source_rows": len(source_data),
        "total_target_rows": len(target_data)
    }
```

**Step 2: Add Generator-Based Approach (2 hours)**

```python
# Add to src/reconciliation/differ.py

def iter_discrepancies(
    self,
    source_data: Iterator[Dict[str, Any]],
    target_data: Iterator[Dict[str, Any]],
    key_field: Union[str, List[str]],
    ignore_fields: Optional[List[str]] = None
) -> Iterator[Dict[str, Any]]:
    """
    Iterate over discrepancies without loading full datasets.

    This method is for very large datasets that don't fit in memory.

    Args:
        source_data: Iterator over source rows
        target_data: Iterator over target rows
        key_field: Key field(s)
        ignore_fields: Fields to ignore

    Yields:
        Discrepancy dictionaries with type and data
    """
    # Build sparse target index (only keys, not full rows)
    target_keys = set()
    target_lookup = {}

    for target_row in target_data:
        key = self._extract_key(target_row, key_field)
        target_keys.add(key)
        target_lookup[key] = target_row

    # Process source rows
    source_keys = set()
    for source_row in source_data:
        key = self._extract_key(source_row, key_field)
        source_keys.add(key)

        if key not in target_keys:
            yield {
                "type": "missing",
                "key": key,
                "row": source_row
            }
        else:
            target_row = target_lookup[key]
            if not self.comparer.compare_rows(source_row, target_row, ignore_fields=ignore_fields):
                yield {
                    "type": "mismatch",
                    "key": key,
                    "scylla": source_row,
                    "postgres": target_row
                }

    # Find extra rows
    extra_keys = target_keys - source_keys
    for key in extra_keys:
        yield {
            "type": "extra",
            "key": key,
            "row": target_lookup[key]
        }
```

**Step 3: Add Tests (2 hours)**

```python
# Add to tests/unit/test_differ.py

def test_streaming_comparison_large_dataset():
    """Test streaming comparison with large dataset."""
    differ = DataDiffer()

    # Generate large dataset (10K rows)
    source = [{"id": i, "value": f"source_{i}"} for i in range(10000)]
    target = [{"id": i, "value": f"target_{i}" if i % 100 == 0 else f"source_{i}"} for i in range(10000)]

    result = differ.find_all_discrepancies_streaming(
        source_data=source,
        target_data=target,
        key_field="id",
        batch_size=1000
    )

    assert result["mismatch_count"] == 100  # Every 100th row
    assert result["missing_count"] == 0
    assert result["extra_count"] == 0


def test_iterator_based_comparison():
    """Test iterator-based comparison for very large datasets."""
    differ = DataDiffer()

    def source_generator():
        for i in range(1000):
            yield {"id": i, "value": f"val_{i}"}

    def target_generator():
        for i in range(500, 1500):  # Offset range
            yield {"id": i, "value": f"val_{i}"}

    discrepancies = list(differ.iter_discrepancies(
        source_data=source_generator(),
        target_data=target_generator(),
        key_field="id"
    ))

    missing = [d for d in discrepancies if d["type"] == "missing"]
    extra = [d for d in discrepancies if d["type"] == "extra"]

    assert len(missing) == 500  # 0-499 missing in target
    assert len(extra) == 500    # 1000-1499 extra in target
```

---

### Bug #6: Schema Validation Missing Required Fields

**Priority:** MEDIUM
**Effort:** 0.5 days
**Files Affected:**
- `src/utils/schema_validator.py`
- `tests/unit/test_schema_validator.py`

#### Implementation Steps

**Step 1: Add Namespace Validation (30 min)**

```python
# Update src/utils/schema_validator.py:158-200

def validate_avro_schema(self, schema: Dict[str, Any], strict: bool = False) -> bool:
    """
    Validate an Avro schema structure.

    Args:
        schema: Avro schema dictionary
        strict: If True, enforce best practices (e.g., namespace)

    Returns:
        True if schema is valid

    Raises:
        SchemaValidationError: If schema is invalid
    """
    if not isinstance(schema, dict):
        raise SchemaValidationError("Schema must be a dictionary")

    # Check required fields
    required_fields = ["type", "name"]
    missing_fields = [field for field in required_fields if field not in schema]

    if missing_fields:
        raise SchemaValidationError(f"Schema missing required fields: {missing_fields}")

    # Validate type
    valid_types = ["record", "enum", "array", "map", "fixed"]
    schema_type = schema.get("type")

    if schema_type not in valid_types and not isinstance(schema_type, dict):
        raise SchemaValidationError(f"Invalid schema type: {schema_type}")

    # Check for namespace (best practice)
    if schema_type == "record" and "namespace" not in schema:
        if strict:
            raise SchemaValidationError(
                f"Record schema '{schema.get('name')}' missing 'namespace' field. "
                "Namespaces prevent naming conflicts in multi-team environments."
            )
        else:
            logger.warning(
                f"Schema '{schema.get('name')}' missing recommended 'namespace' field. "
                "Consider adding namespace for better schema organization."
            )

    # Validate fields if it's a record type
    if schema_type == "record":
        if "fields" not in schema:
            raise SchemaValidationError("Record schema must have 'fields' property")

        if not isinstance(schema["fields"], list):
            raise SchemaValidationError("Schema 'fields' must be a list")

        for field in schema["fields"]:
            self._validate_field(field)

    logger.debug(f"Schema validation passed for: {schema.get('name')}")
    return True
```

**Step 2: Add Tests (1 hour)**

```python
# Add to tests/unit/test_schema_validator.py

def test_schema_without_namespace_warns():
    """Test that schema without namespace issues warning."""
    validator = SchemaValidator()

    schema = {
        "type": "record",
        "name": "User",
        # Missing namespace
        "fields": [
            {"name": "id", "type": "int"}
        ]
    }

    with pytest.warns(UserWarning, match="missing recommended 'namespace'"):
        result = validator.validate_avro_schema(schema, strict=False)
        assert result is True


def test_schema_without_namespace_strict_mode_fails():
    """Test that strict mode requires namespace."""
    validator = SchemaValidator()

    schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "int"}
        ]
    }

    with pytest.raises(SchemaValidationError) as exc_info:
        validator.validate_avro_schema(schema, strict=True)

    assert "missing 'namespace' field" in str(exc_info.value)
    assert "prevent naming conflicts" in str(exc_info.value)


def test_schema_with_namespace_passes():
    """Test that schema with namespace passes validation."""
    validator = SchemaValidator()

    schema = {
        "type": "record",
        "name": "User",
        "namespace": "com.example.users",
        "fields": [
            {"name": "id", "type": "int"}
        ]
    }

    result = validator.validate_avro_schema(schema, strict=True)
    assert result is True
```

---

## Low Priority Bugs

### Bug #7: Health Check Return Value Inconsistency

**Priority:** LOW
**Effort:** 0.25 days
**Files Affected:**
- `src/utils/vault_client.py`
- `tests/unit/test_vault_client.py`

#### Implementation Steps

**Step 1: Return Structured Health Status (1 hour)**

```python
# Update src/utils/vault_client.py:172-196

from dataclasses import dataclass
from typing import Optional

@dataclass
class HealthStatus:
    """Health check result."""
    healthy: bool
    authenticated: bool
    sealed: bool
    error: Optional[str] = None

    def __bool__(self):
        """Allow boolean checks: if health_status: ..."""
        return self.healthy


def health_check(self) -> HealthStatus:
    """
    Check if Vault is accessible and authenticated.

    Returns:
        HealthStatus object with detailed status

    Example:
        >>> health = vault_client.health_check()
        >>> if health:
        ...     print("Vault is healthy")
        >>> else:
        ...     print(f"Vault unhealthy: {health.error}")
    """
    try:
        if not self.client.is_authenticated():
            logger.warning("Vault authentication check failed")
            return HealthStatus(
                healthy=False,
                authenticated=False,
                sealed=False,
                error="Not authenticated"
            )

        health = self.client.sys.read_health_status()
        is_sealed = health.get("sealed", True)
        is_healthy = not is_sealed

        if is_healthy:
            logger.info("Vault health check passed")
            return HealthStatus(
                healthy=True,
                authenticated=True,
                sealed=False
            )
        else:
            logger.warning("Vault is sealed")
            return HealthStatus(
                healthy=False,
                authenticated=True,
                sealed=True,
                error="Vault is sealed"
            )

    except Exception as e:
        logger.error(f"Vault health check failed: {e}")
        return HealthStatus(
            healthy=False,
            authenticated=False,
            sealed=False,
            error=str(e)
        )
```

---

### Bug #8: Missing Test for Empty Dataset Edge Case

**Priority:** LOW
**Effort:** 0.25 days
**Files Affected:**
- `src/reconciliation/differ.py`
- `tests/unit/test_differ.py`

#### Implementation Steps

**Step 1: Fix Schema Detection (30 min)**

```python
# Update src/reconciliation/differ.py:435-467

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
```

**Step 2: Add Tests (30 min)**

```python
# Add to tests/unit/test_differ.py

def test_schema_differences_with_sparse_fields():
    """Test schema detection when rows have different fields."""
    differ = DataDiffer()

    source = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob"},  # Missing email
        {"id": 3, "age": 30}       # Missing name and email
    ]

    target = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob", "phone": "123-456"}
    ]

    result = differ.find_schema_differences(source, target)

    # Should aggregate all fields across all rows
    assert set(result["only_in_source"]) == {"email", "age"}
    assert result["only_in_target"] == ["phone"]
    assert set(result["common_fields"]) == {"id", "name"}
```

---

## Testing Strategy

### Unit Test Requirements

- [ ] **Minimum 80% code coverage** maintained
- [ ] **All edge cases tested** (NULL, empty, special characters)
- [ ] **Error conditions tested** (exceptions, invalid input)
- [ ] **Performance regression tests** (ensure fixes don't degrade performance)

### Integration Test Requirements

- [ ] **End-to-end tests** with real PostgreSQL database
- [ ] **Schema validation** with real Kafka Schema Registry
- [ ] **Reconciliation tests** with actual discrepancies
- [ ] **Error recovery tests** (network failures, timeouts)

### Test Execution Plan

```bash
# Run unit tests only (fast)
pytest tests/unit/ -v

# Run specific bug fix tests
pytest tests/unit/test_repairer.py::test_insert_sql_with_reserved_keywords -v

# Run integration tests (slow)
pytest tests/integration/ -v

# Run all tests with coverage
pytest --cov=src --cov-report=html --cov-fail-under=80
```

---

## Rollout Plan

### Phase 1: High Priority Bugs (Week 1)

**Day 1-2: Bug #1 (SQL Injection)**
- Implement identifier quoting
- Add comprehensive tests
- Code review

**Day 3: Bug #2 (Key Validation)**
- Add key validation
- Test edge cases
- Code review

**Day 4-5: Bug #3 (Data Types)**
- Add type handlers
- Integration testing
- Code review and merge

### Phase 2: Medium Priority Bugs (Week 2)

**Day 1: Bug #4 (Float Tolerance)**
- Fix side effect
- Thread safety tests

**Day 2-3: Bug #5 (Batch Processing)**
- Implement streaming
- Performance testing

**Day 4: Bug #6 (Schema Validation)**
- Add namespace check
- Update documentation

### Phase 3: Low Priority Bugs (Week 3)

**Day 1: Bugs #7 and #8**
- Structured health status
- Schema aggregation
- Final testing and documentation

---

## Success Metrics

### Code Quality Metrics

- [ ] **Test coverage ≥ 80%** maintained across all modules
- [ ] **Zero critical bugs** remaining
- [ ] **Zero high priority bugs** remaining
- [ ] **All CI/CD checks** passing

### Performance Metrics

- [ ] **Reconciliation performance** unchanged (±5%)
- [ ] **SQL generation overhead** < 1%
- [ ] **Memory usage** for batch processing reduced by ≥ 50%

### Reliability Metrics

- [ ] **Zero SQL injection** vulnerabilities (verified by security scan)
- [ ] **Zero silent failures** in key extraction
- [ ] **100% data type coverage** for common PostgreSQL types

---

## Risk Mitigation

### High Risk Mitigations

1. **SQL Generation Changes (Bug #1)**
   - Feature flag to enable/disable quoting
   - Extensive testing with production data snapshots
   - Gradual rollout with monitoring

2. **Breaking API Changes**
   - Maintain backward compatibility with `quote_identifiers` parameter
   - Deprecation warnings for old behavior
   - Migration guide in documentation

### Medium Risk Mitigations

1. **Performance Regressions**
   - Benchmark before/after for each bug fix
   - Performance tests in CI/CD
   - Rollback plan if performance degrades > 10%

2. **Data Integrity**
   - Dry-run mode for all repair actions
   - Reconciliation verification after fixes
   - Audit logging for all SQL generation

---

## Appendix

### Related Documents

- [SWOT Analysis v0.0](SWOT_v0.0.md)
- [Architecture Documentation](architecture.md)
- [Testing Guide](TESTING.md)
- [Troubleshooting Guide](troubleshooting.md)

### Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-17 | Claude Sonnet 4.5 | Initial implementation plan |

---

**End of Implementation Plan**
