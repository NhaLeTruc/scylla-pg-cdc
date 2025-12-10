"""
Metrics Collector Utility for CDC Pipeline

Provides Prometheus metrics collection for custom CDC pipeline components
and utilities.
"""

from typing import Dict, Optional
from prometheus_client import Counter, Gauge, Histogram, Summary, CollectorRegistry, push_to_gateway
import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


class MetricsCollector:
    """
    Collects and exposes Prometheus metrics for CDC pipeline operations.

    Provides convenient methods to track counters, gauges, histograms,
    and summaries for monitoring pipeline health and performance.
    """

    def __init__(
        self,
        namespace: str = "cdc_pipeline",
        registry: Optional[CollectorRegistry] = None
    ):
        """
        Initialize metrics collector.

        Args:
            namespace: Metric namespace prefix
            registry: Prometheus registry (uses default if not provided)
        """
        self.namespace = namespace
        self.registry = registry or CollectorRegistry()
        self._metrics: Dict[str, any] = {}

        logger.info(f"Initialized MetricsCollector with namespace: {namespace}")

    def create_counter(
        self,
        name: str,
        description: str,
        labels: Optional[list] = None
    ) -> Counter:
        """
        Create or retrieve a counter metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Optional list of label names

        Returns:
            Prometheus Counter instance
        """
        full_name = f"{self.namespace}_{name}"

        if full_name not in self._metrics:
            self._metrics[full_name] = Counter(
                full_name,
                description,
                labelnames=labels or [],
                registry=self.registry
            )
            logger.debug(f"Created counter: {full_name}")

        return self._metrics[full_name]

    def create_gauge(
        self,
        name: str,
        description: str,
        labels: Optional[list] = None
    ) -> Gauge:
        """
        Create or retrieve a gauge metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Optional list of label names

        Returns:
            Prometheus Gauge instance
        """
        full_name = f"{self.namespace}_{name}"

        if full_name not in self._metrics:
            self._metrics[full_name] = Gauge(
                full_name,
                description,
                labelnames=labels or [],
                registry=self.registry
            )
            logger.debug(f"Created gauge: {full_name}")

        return self._metrics[full_name]

    def create_histogram(
        self,
        name: str,
        description: str,
        labels: Optional[list] = None,
        buckets: Optional[tuple] = None
    ) -> Histogram:
        """
        Create or retrieve a histogram metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Optional list of label names
            buckets: Optional histogram buckets

        Returns:
            Prometheus Histogram instance
        """
        full_name = f"{self.namespace}_{name}"

        if full_name not in self._metrics:
            kwargs = {
                "name": full_name,
                "documentation": description,
                "labelnames": labels or [],
                "registry": self.registry
            }

            if buckets:
                kwargs["buckets"] = buckets

            self._metrics[full_name] = Histogram(**kwargs)
            logger.debug(f"Created histogram: {full_name}")

        return self._metrics[full_name]

    def create_summary(
        self,
        name: str,
        description: str,
        labels: Optional[list] = None
    ) -> Summary:
        """
        Create or retrieve a summary metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Optional list of label names

        Returns:
            Prometheus Summary instance
        """
        full_name = f"{self.namespace}_{name}"

        if full_name not in self._metrics:
            self._metrics[full_name] = Summary(
                full_name,
                description,
                labelnames=labels or [],
                registry=self.registry
            )
            logger.debug(f"Created summary: {full_name}")

        return self._metrics[full_name]

    def increment_counter(self, name: str, value: float = 1.0, labels: Optional[Dict] = None) -> None:
        """
        Increment a counter metric.

        Args:
            name: Counter name (without namespace prefix)
            value: Amount to increment by
            labels: Optional label values
        """
        full_name = f"{self.namespace}_{name}"

        if full_name in self._metrics:
            counter = self._metrics[full_name]

            if labels:
                counter.labels(**labels).inc(value)
            else:
                counter.inc(value)

            logger.debug(f"Incremented counter {full_name} by {value}")
        else:
            logger.warning(f"Counter {full_name} not found")

    def set_gauge(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        """
        Set a gauge metric value.

        Args:
            name: Gauge name (without namespace prefix)
            value: Value to set
            labels: Optional label values
        """
        full_name = f"{self.namespace}_{name}"

        if full_name in self._metrics:
            gauge = self._metrics[full_name]

            if labels:
                gauge.labels(**labels).set(value)
            else:
                gauge.set(value)

            logger.debug(f"Set gauge {full_name} to {value}")
        else:
            logger.warning(f"Gauge {full_name} not found")

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict] = None) -> None:
        """
        Record a value in a histogram metric.

        Args:
            name: Histogram name (without namespace prefix)
            value: Value to observe
            labels: Optional label values
        """
        full_name = f"{self.namespace}_{name}"

        if full_name in self._metrics:
            histogram = self._metrics[full_name]

            if labels:
                histogram.labels(**labels).observe(value)
            else:
                histogram.observe(value)

            logger.debug(f"Observed {value} in histogram {full_name}")
        else:
            logger.warning(f"Histogram {full_name} not found")

    def time_function(self, metric_name: str, labels: Optional[Dict] = None):
        """
        Decorator to time function execution and record in histogram.

        Args:
            metric_name: Name of histogram metric to record timing
            labels: Optional label values

        Returns:
            Decorator function
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    duration = time.time() - start_time
                    self.observe_histogram(metric_name, duration, labels)
                    logger.debug(f"Function {func.__name__} took {duration:.3f}s")
            return wrapper
        return decorator

    def push_to_gateway(
        self,
        gateway_url: str,
        job_name: str,
        grouping_key: Optional[Dict] = None
    ) -> None:
        """
        Push metrics to Prometheus Pushgateway.

        Args:
            gateway_url: Pushgateway URL
            job_name: Job name for metrics
            grouping_key: Optional grouping key labels

        Raises:
            Exception: If push fails
        """
        try:
            push_to_gateway(
                gateway_url,
                job=job_name,
                registry=self.registry,
                grouping_key=grouping_key or {}
            )
            logger.info(f"Pushed metrics to gateway: {gateway_url}")
        except Exception as e:
            logger.error(f"Failed to push metrics to gateway: {e}")
            raise

    def get_metric(self, name: str):
        """
        Retrieve a metric by name.

        Args:
            name: Metric name (with or without namespace)

        Returns:
            Metric instance or None
        """
        # First try the name as-is
        if name in self._metrics:
            return self._metrics[name]
        # Then try with namespace prefix
        full_name = f"{self.namespace}_{name}"
        return self._metrics.get(full_name)

    def clear_metrics(self) -> None:
        """Clear all registered metrics."""
        self._metrics.clear()
        logger.info("Cleared all metrics")


# Global metrics collector instance
_default_collector: Optional[MetricsCollector] = None


def get_default_collector() -> MetricsCollector:
    """
    Get or create the default metrics collector instance.

    Returns:
        Default MetricsCollector instance
    """
    global _default_collector

    if _default_collector is None:
        _default_collector = MetricsCollector()
        logger.info("Created default metrics collector")

    return _default_collector


def setup_cdc_metrics(collector: Optional[MetricsCollector] = None) -> MetricsCollector:
    """
    Set up standard CDC pipeline metrics.

    Args:
        collector: Optional collector instance (uses default if not provided)

    Returns:
        Configured MetricsCollector instance
    """
    collector = collector or get_default_collector()

    # Counters
    collector.create_counter(
        "records_processed_total",
        "Total number of records processed",
        labels=["source", "table", "status"]
    )

    collector.create_counter(
        "errors_total",
        "Total number of errors",
        labels=["component", "error_type"]
    )

    collector.create_counter(
        "schema_validations_total",
        "Total number of schema validations",
        labels=["result"]
    )

    # Gauges
    collector.create_gauge(
        "active_connections",
        "Number of active database connections",
        labels=["database"]
    )

    collector.create_gauge(
        "queue_size",
        "Current size of processing queue",
        labels=["queue_name"]
    )

    collector.create_gauge(
        "lag_seconds",
        "Replication lag in seconds",
        labels=["source", "target"]
    )

    # Histograms
    collector.create_histogram(
        "processing_duration_seconds",
        "Time spent processing records",
        labels=["operation"],
        buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0)
    )

    collector.create_histogram(
        "batch_size",
        "Size of processing batches",
        labels=["operation"],
        buckets=(1, 10, 50, 100, 500, 1000, 5000, 10000)
    )

    # Summaries
    collector.create_summary(
        "record_size_bytes",
        "Size of records in bytes",
        labels=["table"]
    )

    logger.info("Set up CDC pipeline metrics")
    return collector
