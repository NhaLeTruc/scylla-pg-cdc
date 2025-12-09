"""
Monitoring Module for CDC Pipeline

This module provides observability components for the ScyllaDB to PostgreSQL
CDC pipeline, including:
- Custom Prometheus metrics
- Alert rule definitions
- Metric exporters

Usage:
    from src.monitoring import MetricsCollector, AlertRuleGenerator

    # Collect custom metrics
    metrics = MetricsCollector()
    metrics.record_reconciliation_run(
        table="users",
        discrepancies={"missing": 10, "extra": 5, "mismatches": 3},
        duration_seconds=45.2
    )

    # Generate alert rules
    alerts = AlertRuleGenerator()
    rules = alerts.generate_alert_rules()
"""

from src.monitoring.metrics import MetricsCollector, ReconciliationMetrics
from src.monitoring.alerts import AlertRuleGenerator

__all__ = [
    "MetricsCollector",
    "ReconciliationMetrics",
    "AlertRuleGenerator",
]

__version__ = "1.0.0"
