"""
Prometheus Metrics for CDC Pipeline

Custom metrics for tracking reconciliation, replication health, and performance.
Metrics are exposed on port 9090 for Prometheus scraping.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from prometheus_client import Counter, Gauge, Histogram, Summary, Info, start_http_server
import os

logger = logging.getLogger(__name__)


class ReconciliationMetrics:
    """Prometheus metrics for reconciliation operations."""

    def __init__(self):
        """Initialize reconciliation metrics."""

        # Reconciliation run counter
        self.reconciliation_runs_total = Counter(
            'cdc_reconciliation_runs_total',
            'Total number of reconciliation runs',
            ['table', 'mode', 'status']
        )

        # Discrepancy counters
        self.discrepancies_found_total = Counter(
            'cdc_discrepancies_found_total',
            'Total discrepancies found by type',
            ['table', 'discrepancy_type']
        )

        # Repair actions
        self.repair_actions_total = Counter(
            'cdc_repair_actions_total',
            'Total repair actions executed',
            ['table', 'action_type', 'status']
        )

        # Reconciliation duration
        self.reconciliation_duration_seconds = Histogram(
            'cdc_reconciliation_duration_seconds',
            'Duration of reconciliation runs in seconds',
            ['table', 'mode'],
            buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600]
        )

        # Current discrepancy gauges
        self.current_missing_rows = Gauge(
            'cdc_current_missing_rows',
            'Current number of missing rows',
            ['table']
        )

        self.current_extra_rows = Gauge(
            'cdc_current_extra_rows',
            'Current number of extra rows',
            ['table']
        )

        self.current_mismatched_rows = Gauge(
            'cdc_current_mismatched_rows',
            'Current number of mismatched rows',
            ['table']
        )

        # Accuracy gauge
        self.data_accuracy_percentage = Gauge(
            'cdc_data_accuracy_percentage',
            'Data accuracy percentage (0-100)',
            ['table']
        )

        # Rows processed
        self.rows_processed_total = Counter(
            'cdc_rows_processed_total',
            'Total rows processed during reconciliation',
            ['table', 'source']
        )

        logger.info("ReconciliationMetrics initialized")

    def record_reconciliation_run(
        self,
        table: str,
        mode: str,
        status: str,
        duration_seconds: float,
        discrepancies: Dict[str, int],
        total_source_rows: int,
        total_target_rows: int
    ) -> None:
        """
        Record a reconciliation run.

        Args:
            table: Table name
            mode: Reconciliation mode (full/incremental)
            status: Run status (success/failure)
            duration_seconds: Duration in seconds
            discrepancies: Dict with missing_count, extra_count, mismatch_count
            total_source_rows: Total source rows
            total_target_rows: Total target rows
        """
        # Increment run counter
        self.reconciliation_runs_total.labels(
            table=table,
            mode=mode,
            status=status
        ).inc()

        # Record duration
        self.reconciliation_duration_seconds.labels(
            table=table,
            mode=mode
        ).observe(duration_seconds)

        # Record discrepancies
        missing = discrepancies.get('missing_count', 0)
        extra = discrepancies.get('extra_count', 0)
        mismatches = discrepancies.get('mismatch_count', 0)

        self.discrepancies_found_total.labels(
            table=table,
            discrepancy_type='missing'
        ).inc(missing)

        self.discrepancies_found_total.labels(
            table=table,
            discrepancy_type='extra'
        ).inc(extra)

        self.discrepancies_found_total.labels(
            table=table,
            discrepancy_type='mismatch'
        ).inc(mismatches)

        # Update current gauges
        self.current_missing_rows.labels(table=table).set(missing)
        self.current_extra_rows.labels(table=table).set(extra)
        self.current_mismatched_rows.labels(table=table).set(mismatches)

        # Calculate and set accuracy
        if total_source_rows > 0:
            issues = missing + mismatches
            accuracy = ((total_source_rows - issues) / total_source_rows) * 100
            self.data_accuracy_percentage.labels(table=table).set(accuracy)

        # Record rows processed
        self.rows_processed_total.labels(
            table=table,
            source='scylladb'
        ).inc(total_source_rows)

        self.rows_processed_total.labels(
            table=table,
            source='postgres'
        ).inc(total_target_rows)

        logger.debug(
            f"Recorded reconciliation metrics for {table}: "
            f"mode={mode}, status={status}, duration={duration_seconds}s, "
            f"discrepancies={discrepancies}"
        )

    def record_repair_action(
        self,
        table: str,
        action_type: str,
        status: str
    ) -> None:
        """
        Record a repair action.

        Args:
            table: Table name
            action_type: Action type (INSERT/UPDATE/DELETE)
            status: Action status (success/failure)
        """
        self.repair_actions_total.labels(
            table=table,
            action_type=action_type,
            status=status
        ).inc()


class ReplicationMetrics:
    """Prometheus metrics for CDC replication operations."""

    def __init__(self):
        """Initialize replication metrics."""

        # Replication lag
        self.replication_lag_seconds = Gauge(
            'cdc_replication_lag_seconds',
            'Replication lag in seconds',
            ['table', 'connector']
        )

        # Records replicated
        self.records_replicated_total = Counter(
            'cdc_records_replicated_total',
            'Total records replicated',
            ['table', 'connector', 'status']
        )

        # Replication errors
        self.replication_errors_total = Counter(
            'cdc_replication_errors_total',
            'Total replication errors',
            ['table', 'connector', 'error_type']
        )

        # DLQ messages
        self.dlq_messages_total = Gauge(
            'cdc_dlq_messages_total',
            'Total messages in Dead Letter Queue',
            ['topic']
        )

        # Connector health
        self.connector_healthy = Gauge(
            'cdc_connector_healthy',
            'Connector health status (1=healthy, 0=unhealthy)',
            ['connector']
        )

        # Throughput
        self.replication_throughput = Summary(
            'cdc_replication_throughput_records_per_second',
            'Replication throughput in records per second',
            ['table', 'connector']
        )

        logger.info("ReplicationMetrics initialized")

    def update_replication_lag(
        self,
        table: str,
        connector: str,
        lag_seconds: float
    ) -> None:
        """Update replication lag metric."""
        self.replication_lag_seconds.labels(
            table=table,
            connector=connector
        ).set(lag_seconds)

    def record_replicated_record(
        self,
        table: str,
        connector: str,
        status: str = 'success'
    ) -> None:
        """Record a replicated record."""
        self.records_replicated_total.labels(
            table=table,
            connector=connector,
            status=status
        ).inc()

    def record_replication_error(
        self,
        table: str,
        connector: str,
        error_type: str
    ) -> None:
        """Record a replication error."""
        self.replication_errors_total.labels(
            table=table,
            connector=connector,
            error_type=error_type
        ).inc()

    def update_dlq_count(
        self,
        topic: str,
        count: int
    ) -> None:
        """Update DLQ message count."""
        self.dlq_messages_total.labels(topic=topic).set(count)

    def update_connector_health(
        self,
        connector: str,
        is_healthy: bool
    ) -> None:
        """Update connector health status."""
        self.connector_healthy.labels(connector=connector).set(1 if is_healthy else 0)

    def record_throughput(
        self,
        table: str,
        connector: str,
        records_per_second: float
    ) -> None:
        """Record replication throughput."""
        self.replication_throughput.labels(
            table=table,
            connector=connector
        ).observe(records_per_second)


class SchemaMetrics:
    """Prometheus metrics for schema evolution tracking."""

    def __init__(self):
        """Initialize schema metrics."""

        # Schema version info
        self.schema_version_info = Info(
            'cdc_schema_version',
            'Current schema version information'
        )

        # Schema changes
        self.schema_changes_total = Counter(
            'cdc_schema_changes_total',
            'Total schema changes',
            ['table', 'change_type', 'compatibility']
        )

        # Schema compatibility check failures
        self.schema_compatibility_failures_total = Counter(
            'cdc_schema_compatibility_failures_total',
            'Total schema compatibility check failures',
            ['table', 'compatibility_mode']
        )

        # Current schema version
        self.current_schema_version = Gauge(
            'cdc_current_schema_version',
            'Current schema version number',
            ['table', 'subject']
        )

        logger.info("SchemaMetrics initialized")

    def record_schema_change(
        self,
        table: str,
        change_type: str,
        compatibility: str
    ) -> None:
        """
        Record a schema change.

        Args:
            table: Table name
            change_type: Type of change (add_column, remove_column, etc.)
            compatibility: Compatibility level (backward, forward, full, breaking)
        """
        self.schema_changes_total.labels(
            table=table,
            change_type=change_type,
            compatibility=compatibility
        ).inc()

    def record_compatibility_failure(
        self,
        table: str,
        compatibility_mode: str
    ) -> None:
        """Record a schema compatibility check failure."""
        self.schema_compatibility_failures_total.labels(
            table=table,
            compatibility_mode=compatibility_mode
        ).inc()

    def update_schema_version(
        self,
        table: str,
        subject: str,
        version: int
    ) -> None:
        """Update current schema version."""
        self.current_schema_version.labels(
            table=table,
            subject=subject
        ).set(version)


class MetricsCollector:
    """
    Main metrics collector for CDC pipeline.

    Combines all metric categories and provides unified interface.
    """

    def __init__(self, port: int = 9090):
        """
        Initialize metrics collector.

        Args:
            port: Port for Prometheus metrics server
        """
        self.port = port
        self.reconciliation = ReconciliationMetrics()
        self.replication = ReplicationMetrics()
        self.schema = SchemaMetrics()

        # System info
        self.pipeline_info = Info('cdc_pipeline_info', 'CDC pipeline information')
        self.pipeline_info.info({
            'version': '1.0.0',
            'source': 'scylladb',
            'target': 'postgresql',
            'framework': 'kafka-connect'
        })

        logger.info(f"MetricsCollector initialized on port {port}")

    def start_server(self) -> None:
        """Start Prometheus metrics HTTP server."""
        try:
            start_http_server(self.port)
            logger.info(f"Metrics server started on port {self.port}")
        except OSError as e:
            if "Address already in use" in str(e):
                logger.warning(f"Metrics server already running on port {self.port}")
            else:
                raise

    def record_reconciliation_run(self, **kwargs) -> None:
        """Record reconciliation run (delegates to ReconciliationMetrics)."""
        self.reconciliation.record_reconciliation_run(**kwargs)

    def record_repair_action(self, **kwargs) -> None:
        """Record repair action (delegates to ReconciliationMetrics)."""
        self.reconciliation.record_repair_action(**kwargs)

    def update_replication_lag(self, **kwargs) -> None:
        """Update replication lag (delegates to ReplicationMetrics)."""
        self.replication.update_replication_lag(**kwargs)

    def record_schema_change(self, **kwargs) -> None:
        """Record schema change (delegates to SchemaMetrics)."""
        self.schema.record_schema_change(**kwargs)


# Singleton instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector(port: int = 9090) -> MetricsCollector:
    """
    Get or create singleton metrics collector.

    Args:
        port: Port for metrics server

    Returns:
        MetricsCollector instance
    """
    global _metrics_collector

    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(port=port)

    return _metrics_collector
