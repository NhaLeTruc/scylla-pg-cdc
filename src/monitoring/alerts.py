"""
Alert Rule Generator for Prometheus AlertManager

Generates alert rule definitions for CDC pipeline monitoring.
Rules cover replication lag, error rates, DLQ issues, and data quality.
"""

import logging
from typing import Dict, List, Any
import yaml

logger = logging.getLogger(__name__)


class AlertRuleGenerator:
    """Generates Prometheus AlertManager alert rules."""

    def __init__(self):
        """Initialize alert rule generator."""
        self.rules = []
        logger.info("AlertRuleGenerator initialized")

    def generate_alert_rules(self) -> Dict[str, Any]:
        """
        Generate complete alert rule configuration.

        Returns:
            Dict with alert rule groups in Prometheus format
        """
        groups = [
            self._generate_replication_alerts(),
            self._generate_reconciliation_alerts(),
            self._generate_schema_alerts(),
            self._generate_dlq_alerts(),
            self._generate_connector_health_alerts(),
        ]

        config = {
            "groups": groups
        }

        logger.info(f"Generated {len(groups)} alert rule groups")
        return config

    def _generate_replication_alerts(self) -> Dict[str, Any]:
        """Generate replication-related alerts."""
        return {
            "name": "cdc_replication",
            "interval": "30s",
            "rules": [
                {
                    "alert": "HighReplicationLag",
                    "expr": "cdc_replication_lag_seconds > 300",
                    "for": "5m",
                    "labels": {
                        "severity": "warning",
                        "component": "replication"
                    },
                    "annotations": {
                        "summary": "High replication lag detected",
                        "description": "Replication lag for {{ $labels.table }} on {{ $labels.connector }} is {{ $value }}s (threshold: 300s)"
                    }
                },
                {
                    "alert": "CriticalReplicationLag",
                    "expr": "cdc_replication_lag_seconds > 900",
                    "for": "5m",
                    "labels": {
                        "severity": "critical",
                        "component": "replication"
                    },
                    "annotations": {
                        "summary": "Critical replication lag",
                        "description": "Replication lag for {{ $labels.table }} on {{ $labels.connector }} is {{ $value }}s (threshold: 900s). Immediate action required!"
                    }
                },
                {
                    "alert": "HighReplicationErrorRate",
                    "expr": "rate(cdc_replication_errors_total[5m]) > 0.1",
                    "for": "5m",
                    "labels": {
                        "severity": "warning",
                        "component": "replication"
                    },
                    "annotations": {
                        "summary": "High replication error rate",
                        "description": "Error rate for {{ $labels.table }} is {{ $value }} errors/sec"
                    }
                },
                {
                    "alert": "ReplicationThroughputDrop",
                    "expr": "rate(cdc_records_replicated_total{status=\"success\"}[5m]) < 10",
                    "for": "10m",
                    "labels": {
                        "severity": "warning",
                        "component": "replication"
                    },
                    "annotations": {
                        "summary": "Replication throughput dropped",
                        "description": "Replication throughput for {{ $labels.table }} is {{ $value }} records/sec (below 10 records/sec threshold)"
                    }
                }
            ]
        }

    def _generate_reconciliation_alerts(self) -> Dict[str, Any]:
        """Generate reconciliation-related alerts."""
        return {
            "name": "cdc_reconciliation",
            "interval": "1m",
            "rules": [
                {
                    "alert": "HighDataDiscrepancy",
                    "expr": "cdc_data_accuracy_percentage < 95",
                    "for": "10m",
                    "labels": {
                        "severity": "warning",
                        "component": "reconciliation"
                    },
                    "annotations": {
                        "summary": "High data discrepancy detected",
                        "description": "Data accuracy for {{ $labels.table }} is {{ $value }}% (below 95% threshold)"
                    }
                },
                {
                    "alert": "CriticalDataDiscrepancy",
                    "expr": "cdc_data_accuracy_percentage < 90",
                    "for": "5m",
                    "labels": {
                        "severity": "critical",
                        "component": "reconciliation"
                    },
                    "annotations": {
                        "summary": "Critical data discrepancy",
                        "description": "Data accuracy for {{ $labels.table }} is {{ $value }}% (below 90% threshold). Immediate reconciliation required!"
                    }
                },
                {
                    "alert": "ReconciliationFailure",
                    "expr": "rate(cdc_reconciliation_runs_total{status=\"failure\"}[1h]) > 0",
                    "for": "5m",
                    "labels": {
                        "severity": "warning",
                        "component": "reconciliation"
                    },
                    "annotations": {
                        "summary": "Reconciliation failures detected",
                        "description": "Reconciliation for {{ $labels.table }} is failing"
                    }
                },
                {
                    "alert": "HighMissingRowCount",
                    "expr": "cdc_current_missing_rows > 1000",
                    "for": "15m",
                    "labels": {
                        "severity": "warning",
                        "component": "reconciliation"
                    },
                    "annotations": {
                        "summary": "High missing row count",
                        "description": "{{ $labels.table }} has {{ $value }} missing rows in target database"
                    }
                },
                {
                    "alert": "HighMismatchRate",
                    "expr": "rate(cdc_discrepancies_found_total{discrepancy_type=\"mismatch\"}[1h]) > 100",
                    "for": "10m",
                    "labels": {
                        "severity": "warning",
                        "component": "reconciliation"
                    },
                    "annotations": {
                        "summary": "High data mismatch rate",
                        "description": "{{ $labels.table }} mismatch rate is {{ $value }} mismatches/hour"
                    }
                }
            ]
        }

    def _generate_schema_alerts(self) -> Dict[str, Any]:
        """Generate schema evolution alerts."""
        return {
            "name": "cdc_schema",
            "interval": "1m",
            "rules": [
                {
                    "alert": "SchemaCompatibilityFailure",
                    "expr": "rate(cdc_schema_compatibility_failures_total[10m]) > 0",
                    "for": "2m",
                    "labels": {
                        "severity": "critical",
                        "component": "schema"
                    },
                    "annotations": {
                        "summary": "Schema compatibility check failed",
                        "description": "Schema compatibility failure for {{ $labels.table }} in {{ $labels.compatibility_mode }} mode"
                    }
                },
                {
                    "alert": "FrequentSchemaChanges",
                    "expr": "rate(cdc_schema_changes_total[1h]) > 5",
                    "for": "10m",
                    "labels": {
                        "severity": "info",
                        "component": "schema"
                    },
                    "annotations": {
                        "summary": "Frequent schema changes detected",
                        "description": "{{ $labels.table }} has {{ $value }} schema changes per hour (above 5/hour threshold)"
                    }
                },
                {
                    "alert": "BreakingSchemaChange",
                    "expr": "rate(cdc_schema_changes_total{compatibility=\"breaking\"}[10m]) > 0",
                    "for": "1m",
                    "labels": {
                        "severity": "critical",
                        "component": "schema"
                    },
                    "annotations": {
                        "summary": "Breaking schema change detected",
                        "description": "BREAKING schema change detected for {{ $labels.table }}. Manual intervention may be required!"
                    }
                }
            ]
        }

    def _generate_dlq_alerts(self) -> Dict[str, Any]:
        """Generate Dead Letter Queue alerts."""
        return {
            "name": "cdc_dlq",
            "interval": "1m",
            "rules": [
                {
                    "alert": "DLQMessagesAccumulating",
                    "expr": "cdc_dlq_messages_total > 100",
                    "for": "10m",
                    "labels": {
                        "severity": "warning",
                        "component": "dlq"
                    },
                    "annotations": {
                        "summary": "DLQ messages accumulating",
                        "description": "DLQ topic {{ $labels.topic }} has {{ $value }} messages (threshold: 100)"
                    }
                },
                {
                    "alert": "CriticalDLQBacklog",
                    "expr": "cdc_dlq_messages_total > 1000",
                    "for": "5m",
                    "labels": {
                        "severity": "critical",
                        "component": "dlq"
                    },
                    "annotations": {
                        "summary": "Critical DLQ backlog",
                        "description": "DLQ topic {{ $labels.topic }} has {{ $value }} messages (threshold: 1000). Investigate root cause immediately!"
                    }
                },
                {
                    "alert": "DLQGrowthRate",
                    "expr": "rate(cdc_dlq_messages_total[5m]) > 10",
                    "for": "5m",
                    "labels": {
                        "severity": "warning",
                        "component": "dlq"
                    },
                    "annotations": {
                        "summary": "DLQ growing rapidly",
                        "description": "DLQ topic {{ $labels.topic }} growing at {{ $value }} messages/sec"
                    }
                }
            ]
        }

    def _generate_connector_health_alerts(self) -> Dict[str, Any]:
        """Generate connector health alerts."""
        return {
            "name": "cdc_connector_health",
            "interval": "30s",
            "rules": [
                {
                    "alert": "ConnectorDown",
                    "expr": "cdc_connector_healthy == 0",
                    "for": "2m",
                    "labels": {
                        "severity": "critical",
                        "component": "connector"
                    },
                    "annotations": {
                        "summary": "Connector is down",
                        "description": "Connector {{ $labels.connector }} is not healthy. Check connector status and logs."
                    }
                },
                {
                    "alert": "ConnectorRestartLoop",
                    "expr": "rate(cdc_connector_healthy[10m]) > 0.1",
                    "for": "5m",
                    "labels": {
                        "severity": "warning",
                        "component": "connector"
                    },
                    "annotations": {
                        "summary": "Connector restart loop detected",
                        "description": "Connector {{ $labels.connector }} is restarting frequently"
                    }
                },
                {
                    "alert": "NoDataReplication",
                    "expr": "rate(cdc_records_replicated_total[10m]) == 0",
                    "for": "15m",
                    "labels": {
                        "severity": "warning",
                        "component": "connector"
                    },
                    "annotations": {
                        "summary": "No data replication activity",
                        "description": "No records have been replicated for {{ $labels.table }} in the last 15 minutes"
                    }
                }
            ]
        }

    def export_to_yaml(self, output_file: str) -> None:
        """
        Export alert rules to YAML file.

        Args:
            output_file: Path to output YAML file
        """
        rules = self.generate_alert_rules()

        with open(output_file, 'w') as f:
            yaml.dump(rules, f, default_flow_style=False, sort_keys=False)

        logger.info(f"Alert rules exported to {output_file}")

    def get_alert_summary(self) -> Dict[str, int]:
        """
        Get summary of alert rules.

        Returns:
            Dict with counts by severity
        """
        rules = self.generate_alert_rules()

        summary = {
            "total_groups": len(rules["groups"]),
            "total_alerts": 0,
            "critical": 0,
            "warning": 0,
            "info": 0
        }

        for group in rules["groups"]:
            for rule in group["rules"]:
                summary["total_alerts"] += 1
                severity = rule["labels"].get("severity", "unknown")
                if severity in summary:
                    summary[severity] += 1

        return summary
