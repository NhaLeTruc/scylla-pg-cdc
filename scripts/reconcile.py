#!/usr/bin/env python3
"""
Data Reconciliation Tool for ScyllaDB to PostgreSQL CDC Pipeline

Compares data between ScyllaDB and PostgreSQL, detects discrepancies,
and generates repair actions with support for:
- Full and incremental reconciliation
- Checkpoint-based resumable processing
- Batch processing for memory efficiency
- Dry-run mode for validation
- Status and reporting

Usage:
    ./scripts/reconcile.py reconcile --table users --mode full
    ./scripts/reconcile.py reconcile --table users --mode incremental --checkpoint last
    ./scripts/reconcile.py reconcile --table users --dry-run
    ./scripts/reconcile.py status
    ./scripts/reconcile.py report --table users
"""

import sys
import os
import argparse
import logging
import logging.handlers
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import psycopg2
from psycopg2.extras import RealDictCursor

from src.reconciliation import RowComparer, DataDiffer, DataRepairer


class StructuredJSONFormatter(logging.Formatter):
    """JSON formatter with correlation ID support."""

    def __init__(self):
        super().__init__()
        self.correlation_id = str(uuid.uuid4())

    def format(self, record):
        log_data = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'correlation_id': getattr(record, 'correlation_id', self.correlation_id),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }

        # Add extra fields
        if hasattr(record, 'table'):
            log_data['table'] = record.table
        if hasattr(record, 'mode'):
            log_data['mode'] = record.mode
        if hasattr(record, 'duration'):
            log_data['duration_seconds'] = record.duration
        if hasattr(record, 'discrepancies'):
            log_data['discrepancies'] = record.discrepancies

        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        return json.dumps(log_data)


# Configure structured logging
json_handler = logging.StreamHandler()
json_handler.setFormatter(StructuredJSONFormatter())

# Human-readable handler for console
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))

# Configure root logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)

# Add JSON handler if JSON_LOGGING env var is set
if os.getenv('JSON_LOGGING', 'false').lower() == 'true':
    logger.addHandler(json_handler)
    logger.propagate = False


class ReconciliationCheckpoint:
    """Manages reconciliation checkpoints for resumable processing."""

    def __init__(self, checkpoint_dir: str = ".reconciliation"):
        """
        Initialize checkpoint manager.

        Args:
            checkpoint_dir: Directory to store checkpoint files
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        logger.debug(f"Checkpoint directory: {self.checkpoint_dir}")

    def save_checkpoint(
        self,
        table_name: str,
        checkpoint_data: Dict[str, Any]
    ) -> None:
        """
        Save reconciliation checkpoint.

        Args:
            table_name: Table being reconciled
            checkpoint_data: Checkpoint data
        """
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"

        checkpoint_data["saved_at"] = datetime.now(timezone.utc).isoformat()

        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)

        logger.info(f"Checkpoint saved: {checkpoint_file}")

    def load_checkpoint(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint for a table.

        Args:
            table_name: Table name

        Returns:
            Checkpoint data or None if not found
        """
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"

        if not checkpoint_file.exists():
            logger.debug(f"No checkpoint found for {table_name}")
            return None

        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)

        logger.info(f"Loaded checkpoint from {checkpoint['saved_at']}")
        return checkpoint

    def clear_checkpoint(self, table_name: str) -> None:
        """
        Clear checkpoint for a table.

        Args:
            table_name: Table name
        """
        checkpoint_file = self.checkpoint_dir / f"{table_name}_checkpoint.json"

        if checkpoint_file.exists():
            checkpoint_file.unlink()
            logger.info(f"Checkpoint cleared for {table_name}")

    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """
        List all checkpoints.

        Returns:
            List of checkpoint info
        """
        checkpoints = []

        for checkpoint_file in self.checkpoint_dir.glob("*_checkpoint.json"):
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
                checkpoints.append({
                    "table": checkpoint_file.stem.replace("_checkpoint", ""),
                    "saved_at": data.get("saved_at"),
                    "progress": data.get("progress", {})
                })

        return checkpoints


class ReconciliationTool:
    """Main reconciliation tool."""

    def __init__(
        self,
        scylla_host: str = "localhost",
        scylla_port: int = 9042,
        scylla_keyspace: str = "app_data",
        postgres_host: str = "localhost",
        postgres_port: int = 5432,
        postgres_db: str = "warehouse",
        postgres_schema: str = "cdc_data",
        postgres_user: str = "postgres",
        postgres_password: str = "postgres",
        batch_size: int = 10000
    ):
        """
        Initialize reconciliation tool.

        Args:
            scylla_host: ScyllaDB host
            scylla_port: ScyllaDB port
            scylla_keyspace: ScyllaDB keyspace
            postgres_host: PostgreSQL host
            postgres_port: PostgreSQL port
            postgres_db: PostgreSQL database
            postgres_schema: PostgreSQL schema
            postgres_user: PostgreSQL username
            postgres_password: PostgreSQL password
            batch_size: Batch size for processing
        """
        self.scylla_host = scylla_host
        self.scylla_port = scylla_port
        self.scylla_keyspace = scylla_keyspace
        self.postgres_host = postgres_host
        self.postgres_port = postgres_port
        self.postgres_db = postgres_db
        self.postgres_schema = postgres_schema
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.batch_size = batch_size

        self.differ = DataDiffer()
        self.repairer = DataRepairer()
        self.checkpoint_manager = ReconciliationCheckpoint()

        logger.info("ReconciliationTool initialized")

    def connect_scylla(self):
        """Connect to ScyllaDB."""
        logger.info(f"Connecting to ScyllaDB at {self.scylla_host}:{self.scylla_port}")
        cluster = Cluster([self.scylla_host], port=self.scylla_port)
        session = cluster.connect(self.scylla_keyspace)
        return cluster, session

    def connect_postgres(self):
        """Connect to PostgreSQL."""
        logger.info(f"Connecting to PostgreSQL at {self.postgres_host}:{self.postgres_port}")
        conn = psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password
        )
        return conn

    def fetch_scylla_data(
        self,
        session,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from ScyllaDB.

        Args:
            session: ScyllaDB session
            table_name: Table name
            limit: Max rows to fetch
            offset: Offset (simulated with token)

        Returns:
            List of rows
        """
        query = f"SELECT * FROM {table_name}"

        if limit:
            query += f" LIMIT {limit}"

        logger.debug(f"Executing ScyllaDB query: {query}")
        result = session.execute(query)

        rows = []
        for row in result:
            row_dict = {}
            for key, value in row._asdict().items():
                row_dict[key] = value
            rows.append(row_dict)

        logger.info(f"Fetched {len(rows)} rows from ScyllaDB {table_name}")
        return rows

    def fetch_postgres_data(
        self,
        conn,
        table_name: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Fetch data from PostgreSQL.

        Args:
            conn: PostgreSQL connection
            table_name: Table name
            limit: Max rows to fetch
            offset: Offset for pagination

        Returns:
            List of rows
        """
        query = f"SELECT * FROM {self.postgres_schema}.{table_name}"

        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"

        logger.debug(f"Executing PostgreSQL query: {query}")

        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = [dict(row) for row in cursor.fetchall()]

        logger.info(f"Fetched {len(rows)} rows from PostgreSQL {table_name}")
        return rows

    def reconcile_table(
        self,
        table_name: str,
        key_field: str,
        mode: str = "full",
        dry_run: bool = False,
        ignore_fields: Optional[List[str]] = None,
        resume: bool = False
    ) -> Dict[str, Any]:
        """
        Reconcile a table between ScyllaDB and PostgreSQL.

        Args:
            table_name: Table to reconcile
            key_field: Primary key field
            mode: "full" or "incremental"
            dry_run: If True, don't execute repairs
            ignore_fields: Fields to ignore in comparison
            resume: Resume from checkpoint

        Returns:
            Reconciliation results
        """
        logger.info(f"Starting {mode} reconciliation for table: {table_name}")
        logger.info(f"Key field: {key_field}, Dry run: {dry_run}")

        start_time = datetime.now(timezone.utc)

        # Check for checkpoint
        checkpoint = None
        if resume:
            checkpoint = self.checkpoint_manager.load_checkpoint(table_name)

        # Connect to databases
        scylla_cluster, scylla_session = self.connect_scylla()
        postgres_conn = self.connect_postgres()

        try:
            # Fetch data in batches
            all_scylla_data = []
            all_postgres_data = []

            offset = 0
            if checkpoint:
                offset = checkpoint.get("progress", {}).get("offset", 0)
                logger.info(f"Resuming from offset: {offset}")

            batch_num = 0
            total_processed = offset

            while True:
                logger.info(f"Processing batch {batch_num + 1} (offset: {total_processed})")

                # Fetch batch
                scylla_batch = self.fetch_scylla_data(
                    scylla_session,
                    table_name,
                    limit=self.batch_size,
                    offset=total_processed
                )

                postgres_batch = self.fetch_postgres_data(
                    postgres_conn,
                    table_name,
                    limit=self.batch_size,
                    offset=total_processed
                )

                all_scylla_data.extend(scylla_batch)
                all_postgres_data.extend(postgres_batch)

                total_processed += self.batch_size
                batch_num += 1

                # Save checkpoint
                self.checkpoint_manager.save_checkpoint(table_name, {
                    "progress": {
                        "offset": total_processed,
                        "batch": batch_num
                    },
                    "mode": mode
                })

                # Stop if we've fetched less than batch size
                if len(scylla_batch) < self.batch_size and len(postgres_batch) < self.batch_size:
                    break

            logger.info(
                f"Total data fetched: ScyllaDB={len(all_scylla_data)}, "
                f"PostgreSQL={len(all_postgres_data)}"
            )

            # Find discrepancies
            logger.info("Analyzing discrepancies...")
            discrepancies = self.differ.find_all_discrepancies(
                all_scylla_data,
                all_postgres_data,
                key_field=key_field,
                ignore_fields=ignore_fields
            )

            # Get summary
            summary = self.differ.get_discrepancy_summary(
                all_scylla_data,
                all_postgres_data,
                key_field=key_field,
                ignore_fields=ignore_fields
            )

            logger.info(f"Discrepancy summary: {summary}")

            # Generate repair actions
            logger.info("Generating repair actions...")
            actions = self.repairer.generate_repair_actions(
                discrepancies,
                table_name=table_name,
                schema=self.postgres_schema,
                key_field=key_field,
                dry_run=dry_run
            )

            # Execute repairs if not dry-run
            executed_actions = []
            if not dry_run and actions:
                logger.info(f"Executing {len(actions)} repair actions...")
                executed_actions = self.execute_repairs(postgres_conn, actions)
                postgres_conn.commit()
                logger.info(f"Executed {len(executed_actions)} repairs successfully")
            elif dry_run:
                logger.info("DRY RUN mode - no changes applied")

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Clear checkpoint on success
            self.checkpoint_manager.clear_checkpoint(table_name)

            results = {
                "table": table_name,
                "mode": mode,
                "dry_run": dry_run,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "summary": summary,
                "discrepancies": {
                    "missing_count": len(discrepancies["missing"]),
                    "extra_count": len(discrepancies["extra"]),
                    "mismatch_count": len(discrepancies["mismatches"])
                },
                "actions_generated": len(actions),
                "actions_executed": len(executed_actions) if not dry_run else 0
            }

            logger.info(f"Reconciliation completed in {duration:.2f}s")

            return results

        finally:
            scylla_cluster.shutdown()
            postgres_conn.close()

    def execute_repairs(
        self,
        conn,
        actions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Execute repair actions.

        Args:
            conn: PostgreSQL connection
            actions: List of repair actions

        Returns:
            List of executed actions
        """
        executed = []

        with conn.cursor() as cursor:
            for i, action in enumerate(actions):
                try:
                    logger.debug(f"Executing action {i+1}/{len(actions)}: {action['action_type']}")
                    cursor.execute(action["sql"])
                    action["status"] = "executed"
                    action["executed_at"] = datetime.now(timezone.utc).isoformat()
                    executed.append(action)

                except Exception as e:
                    logger.error(f"Failed to execute action: {e}")
                    logger.error(f"SQL: {action['sql']}")
                    action["status"] = "failed"
                    action["error"] = str(e)

        return executed

    def get_status(self) -> Dict[str, Any]:
        """
        Get reconciliation status.

        Returns:
            Status information
        """
        checkpoints = self.checkpoint_manager.list_checkpoints()

        return {
            "active_checkpoints": len(checkpoints),
            "checkpoints": checkpoints
        }

    def generate_report(
        self,
        table_name: str,
        key_field: str
    ) -> Dict[str, Any]:
        """
        Generate reconciliation report for a table.

        Args:
            table_name: Table name
            key_field: Key field

        Returns:
            Report data
        """
        logger.info(f"Generating report for table: {table_name}")

        scylla_cluster, scylla_session = self.connect_scylla()
        postgres_conn = self.connect_postgres()

        try:
            # Fetch data
            scylla_data = self.fetch_scylla_data(scylla_session, table_name)
            postgres_data = self.fetch_postgres_data(postgres_conn, table_name)

            # Get summary
            summary = self.differ.get_discrepancy_summary(
                scylla_data,
                postgres_data,
                key_field=key_field
            )

            # Calculate accuracy
            total = summary["total_source_rows"]
            issues = summary["missing_count"] + summary["mismatch_count"]
            accuracy = ((total - issues) / total * 100) if total > 0 else 100.0

            report = {
                "table": table_name,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "summary": summary,
                "accuracy_percentage": round(accuracy, 2),
                "recommendation": self._get_recommendation(summary)
            }

            return report

        finally:
            scylla_cluster.shutdown()
            postgres_conn.close()

    def _get_recommendation(self, summary: Dict[str, int]) -> str:
        """Get recommendation based on summary."""
        total = summary["total_source_rows"]
        issues = summary["missing_count"] + summary["mismatch_count"]

        if issues == 0:
            return "No action needed - data is fully synchronized"

        issue_pct = (issues / total * 100) if total > 0 else 0

        if issue_pct < 1:
            return "Minor discrepancies detected - schedule reconciliation during maintenance window"
        elif issue_pct < 5:
            return "Moderate discrepancies detected - reconciliation recommended"
        else:
            return "Significant discrepancies detected - immediate reconciliation required"


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Data Reconciliation Tool for CDC Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Reconcile command
    reconcile_parser = subparsers.add_parser("reconcile", help="Reconcile a table")
    reconcile_parser.add_argument("--table", required=True, help="Table name")
    reconcile_parser.add_argument("--key-field", default="user_id", help="Primary key field")
    reconcile_parser.add_argument("--mode", choices=["full", "incremental"], default="full")
    reconcile_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    reconcile_parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    reconcile_parser.add_argument("--ignore-fields", nargs="+", help="Fields to ignore")
    reconcile_parser.add_argument("--batch-size", type=int, default=10000, help="Batch size")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show reconciliation status")

    # Report command
    report_parser = subparsers.add_parser("report", help="Generate reconciliation report")
    report_parser.add_argument("--table", required=True, help="Table name")
    report_parser.add_argument("--key-field", default="user_id", help="Primary key field")

    # Connection options
    parser.add_argument("--scylla-host", default="localhost", help="ScyllaDB host")
    parser.add_argument("--postgres-host", default="localhost", help="PostgreSQL host")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.command:
        parser.print_help()
        return 1

    # Initialize tool
    tool = ReconciliationTool(
        scylla_host=args.scylla_host,
        postgres_host=args.postgres_host,
        batch_size=getattr(args, 'batch_size', 10000)
    )

    try:
        if args.command == "reconcile":
            results = tool.reconcile_table(
                table_name=args.table,
                key_field=args.key_field,
                mode=args.mode,
                dry_run=args.dry_run,
                ignore_fields=args.ignore_fields,
                resume=args.resume
            )
            print(json.dumps(results, indent=2))

        elif args.command == "status":
            status = tool.get_status()
            print(json.dumps(status, indent=2))

        elif args.command == "report":
            report = tool.generate_report(
                table_name=args.table,
                key_field=args.key_field
            )
            print(json.dumps(report, indent=2))

        return 0

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        return 1


if __name__ == "__main__":
    sys.exit(main())
