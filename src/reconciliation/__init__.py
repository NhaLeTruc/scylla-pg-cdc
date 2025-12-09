"""
Reconciliation Module for CDC Pipeline

This module provides utilities for reconciling data between ScyllaDB and PostgreSQL,
detecting discrepancies, and generating repair actions.

Main components:
- comparer: Row-level comparison logic
- differ: Discrepancy detection algorithms
- repairer: Repair action generation

Usage:
    from src.reconciliation import RowComparer, DataDiffer, DataRepairer

    # Compare individual rows
    comparer = RowComparer()
    is_equal = comparer.compare_rows(scylla_row, postgres_row)

    # Find all discrepancies
    differ = DataDiffer()
    discrepancies = differ.find_all_discrepancies(scylla_data, postgres_data, key_field="user_id")

    # Generate repair actions
    repairer = DataRepairer()
    actions = repairer.generate_repair_actions(discrepancies)
"""

from src.reconciliation.comparer import RowComparer
from src.reconciliation.differ import DataDiffer
from src.reconciliation.repairer import DataRepairer

__all__ = [
    "RowComparer",
    "DataDiffer",
    "DataRepairer",
]

__version__ = "1.0.0"
