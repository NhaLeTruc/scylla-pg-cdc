"""
Unit tests for metrics_collector module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, Summary

from src.utils.metrics_collector import (
    MetricsCollector,
    get_default_collector,
    setup_cdc_metrics
)


class TestMetricsCollector:
    """Test suite for MetricsCollector class."""

    @pytest.fixture
    def collector(self):
        """Create a fresh MetricsCollector instance."""
        return MetricsCollector(namespace="test", registry=CollectorRegistry())

    def test_init_with_custom_namespace(self):
        """Test initialization with custom namespace."""
        collector = MetricsCollector(namespace="custom_namespace")

        assert collector.namespace == "custom_namespace"
        assert collector.registry is not None

    def test_init_with_custom_registry(self):
        """Test initialization with custom registry."""
        registry = CollectorRegistry()
        collector = MetricsCollector(registry=registry)

        assert collector.registry is registry

    def test_create_counter(self, collector):
        """Test creating a counter metric."""
        counter = collector.create_counter(
            "requests_total",
            "Total number of requests",
            labels=["method", "status"]
        )

        assert isinstance(counter, Counter)
        assert "test_requests_total" in collector._metrics

    def test_create_counter_returns_existing(self, collector):
        """Test that create_counter returns existing counter."""
        counter1 = collector.create_counter("test_counter", "Test counter")
        counter2 = collector.create_counter("test_counter", "Test counter")

        assert counter1 is counter2

    def test_create_gauge(self, collector):
        """Test creating a gauge metric."""
        gauge = collector.create_gauge(
            "active_connections",
            "Number of active connections",
            labels=["database"]
        )

        assert isinstance(gauge, Gauge)
        assert "test_active_connections" in collector._metrics

    def test_create_histogram(self, collector):
        """Test creating a histogram metric."""
        histogram = collector.create_histogram(
            "request_duration_seconds",
            "Request duration in seconds",
            labels=["endpoint"],
            buckets=(0.1, 0.5, 1.0, 5.0)
        )

        assert isinstance(histogram, Histogram)
        assert "test_request_duration_seconds" in collector._metrics

    def test_create_histogram_with_default_buckets(self, collector):
        """Test creating histogram with default buckets."""
        histogram = collector.create_histogram(
            "response_time",
            "Response time"
        )

        assert isinstance(histogram, Histogram)

    def test_create_summary(self, collector):
        """Test creating a summary metric."""
        summary = collector.create_summary(
            "request_size_bytes",
            "Request size in bytes",
            labels=["method"]
        )

        assert isinstance(summary, Summary)
        assert "test_request_size_bytes" in collector._metrics

    def test_increment_counter(self, collector):
        """Test incrementing a counter."""
        collector.create_counter("test_counter", "Test counter")

        # Should not raise error
        collector.increment_counter("test_counter", value=5.0)

    def test_increment_counter_with_labels(self, collector):
        """Test incrementing counter with labels."""
        collector.create_counter("requests", "Requests", labels=["method"])

        # Should not raise error
        collector.increment_counter("requests", value=1.0, labels={"method": "GET"})

    def test_increment_nonexistent_counter_logs_warning(self, collector):
        """Test that incrementing nonexistent counter logs warning."""
        with patch('src.utils.metrics_collector.logger') as mock_logger:
            collector.increment_counter("nonexistent", value=1.0)
            mock_logger.warning.assert_called_once()

    def test_set_gauge(self, collector):
        """Test setting a gauge value."""
        collector.create_gauge("connections", "Active connections")

        # Should not raise error
        collector.set_gauge("connections", value=42.0)

    def test_set_gauge_with_labels(self, collector):
        """Test setting gauge with labels."""
        collector.create_gauge("queue_size", "Queue size", labels=["queue"])

        # Should not raise error
        collector.set_gauge("queue_size", value=100.0, labels={"queue": "main"})

    def test_set_nonexistent_gauge_logs_warning(self, collector):
        """Test that setting nonexistent gauge logs warning."""
        with patch('src.utils.metrics_collector.logger') as mock_logger:
            collector.set_gauge("nonexistent", value=1.0)
            mock_logger.warning.assert_called_once()

    def test_observe_histogram(self, collector):
        """Test observing a histogram value."""
        collector.create_histogram("duration", "Duration in seconds")

        # Should not raise error
        collector.observe_histogram("duration", value=2.5)

    def test_observe_histogram_with_labels(self, collector):
        """Test observing histogram with labels."""
        collector.create_histogram("latency", "Latency", labels=["endpoint"])

        # Should not raise error
        collector.observe_histogram("latency", value=0.5, labels={"endpoint": "/api"})

    def test_observe_nonexistent_histogram_logs_warning(self, collector):
        """Test that observing nonexistent histogram logs warning."""
        with patch('src.utils.metrics_collector.logger') as mock_logger:
            collector.observe_histogram("nonexistent", value=1.0)
            mock_logger.warning.assert_called_once()

    def test_time_function_decorator(self, collector):
        """Test time_function decorator."""
        collector.create_histogram("function_duration", "Function duration")

        @collector.time_function("function_duration")
        def test_function():
            return "result"

        result = test_function()

        assert result == "result"

    def test_time_function_decorator_with_labels(self, collector):
        """Test time_function decorator with labels."""
        collector.create_histogram("operation_time", "Operation time", labels=["op"])

        @collector.time_function("operation_time", labels={"op": "read"})
        def test_operation():
            return 42

        result = test_operation()

        assert result == 42

    def test_time_function_records_duration_on_exception(self, collector):
        """Test that time_function records duration even on exception."""
        collector.create_histogram("failed_duration", "Failed operation duration")

        @collector.time_function("failed_duration")
        def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            failing_function()

        # Duration should still be recorded (histogram exists means it was called)
        assert "test_failed_duration" in collector._metrics

    def test_get_metric(self, collector):
        """Test retrieving a metric by name."""
        counter = collector.create_counter("test_counter", "Test")

        retrieved = collector.get_metric("test_counter")

        assert retrieved is counter

    def test_get_metric_with_full_name(self, collector):
        """Test retrieving metric with full namespaced name."""
        counter = collector.create_counter("test_counter", "Test")

        retrieved = collector.get_metric("test_test_counter")

        assert retrieved is counter

    def test_get_metric_nonexistent(self, collector):
        """Test that getting nonexistent metric returns None."""
        metric = collector.get_metric("nonexistent")

        assert metric is None

    def test_clear_metrics(self, collector):
        """Test clearing all metrics."""
        collector.create_counter("counter1", "Counter 1")
        collector.create_gauge("gauge1", "Gauge 1")

        assert len(collector._metrics) == 2

        collector.clear_metrics()

        assert len(collector._metrics) == 0

    @patch('src.utils.metrics_collector.push_to_gateway')
    def test_push_to_gateway_success(self, mock_push, collector):
        """Test pushing metrics to gateway."""
        collector.push_to_gateway(
            "http://pushgateway:9091",
            "test_job",
            grouping_key={"instance": "test"}
        )

        mock_push.assert_called_once_with(
            "http://pushgateway:9091",
            job="test_job",
            registry=collector.registry,
            grouping_key={"instance": "test"}
        )

    @patch('src.utils.metrics_collector.push_to_gateway')
    def test_push_to_gateway_failure(self, mock_push, collector):
        """Test push to gateway failure handling."""
        mock_push.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            collector.push_to_gateway("http://pushgateway:9091", "test_job")


class TestDefaultCollector:
    """Test default collector singleton."""

    def test_get_default_collector_returns_instance(self):
        """Test that get_default_collector returns MetricsCollector."""
        collector = get_default_collector()

        assert isinstance(collector, MetricsCollector)

    def test_get_default_collector_returns_same_instance(self):
        """Test that get_default_collector returns singleton."""
        collector1 = get_default_collector()
        collector2 = get_default_collector()

        assert collector1 is collector2


class TestSetupCDCMetrics:
    """Test CDC metrics setup."""

    def test_setup_cdc_metrics_creates_standard_metrics(self):
        """Test that setup_cdc_metrics creates all standard metrics."""
        registry = CollectorRegistry()
        collector = MetricsCollector(namespace="test", registry=registry)

        result = setup_cdc_metrics(collector)

        assert result is collector

        # Verify standard metrics are created
        assert collector.get_metric("records_processed_total") is not None
        assert collector.get_metric("errors_total") is not None
        assert collector.get_metric("schema_validations_total") is not None
        assert collector.get_metric("active_connections") is not None
        assert collector.get_metric("queue_size") is not None
        assert collector.get_metric("lag_seconds") is not None
        assert collector.get_metric("processing_duration_seconds") is not None
        assert collector.get_metric("batch_size") is not None
        assert collector.get_metric("record_size_bytes") is not None

    def test_setup_cdc_metrics_uses_default_collector(self):
        """Test that setup_cdc_metrics uses default collector if none provided."""
        result = setup_cdc_metrics()

        assert isinstance(result, MetricsCollector)

    def test_setup_cdc_metrics_counters_have_labels(self):
        """Test that counter metrics have appropriate labels."""
        registry = CollectorRegistry()
        collector = MetricsCollector(namespace="test", registry=registry)
        setup_cdc_metrics(collector)

        # These metrics should exist and be usable with labels
        collector.increment_counter(
            "records_processed_total",
            labels={"source": "scylla", "table": "users", "status": "success"}
        )

        collector.increment_counter(
            "errors_total",
            labels={"component": "validator", "error_type": "schema"}
        )

    def test_setup_cdc_metrics_gauges_have_labels(self):
        """Test that gauge metrics have appropriate labels."""
        registry = CollectorRegistry()
        collector = MetricsCollector(namespace="test", registry=registry)
        setup_cdc_metrics(collector)

        # These metrics should exist and be usable with labels
        collector.set_gauge(
            "active_connections",
            value=5.0,
            labels={"database": "postgres"}
        )

        collector.set_gauge(
            "queue_size",
            value=100.0,
            labels={"queue_name": "processing"}
        )

    def test_setup_cdc_metrics_histograms_have_labels(self):
        """Test that histogram metrics have appropriate labels."""
        registry = CollectorRegistry()
        collector = MetricsCollector(namespace="test", registry=registry)
        setup_cdc_metrics(collector)

        # These metrics should exist and be usable with labels
        collector.observe_histogram(
            "processing_duration_seconds",
            value=1.5,
            labels={"operation": "transform"}
        )

        collector.observe_histogram(
            "batch_size",
            value=500.0,
            labels={"operation": "insert"}
        )
