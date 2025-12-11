# Contract Tests - User Guide

## Overview

Contract tests validate the CDC pipeline's integration points:
- **ScyllaDB CDC Source Connector**: Tests that CDC changes are captured and published to Kafka
- **PostgreSQL JDBC Sink Connector**: Tests that Kafka messages are correctly persisted to PostgreSQL

## Prerequisites

All Docker services must be running:
```bash
docker-compose up -d
```

Verify services are healthy:
```bash
docker ps
```

## Running Contract Tests

### Step 1: Clean Test Environment (IMPORTANT!)

**Always run cleanup before tests** to ensure a fresh state:

```bash
./scripts/cleanup-test-env.sh
```

This script will:
1. Truncate all PostgreSQL CDC tables
2. Truncate all ScyllaDB app_data tables
3. Restart both Kafka connectors
4. Wait for the system to stabilize
5. Verify cleanup was successful

### Step 2: Run Tests

```bash
.venv/bin/pytest tests/contract/ -v
```

Or run specific test files:
```bash
# Test ScyllaDB CDC source connector only
.venv/bin/pytest tests/contract/test_scylla_connector.py -v

# Test PostgreSQL sink connector only
.venv/bin/pytest tests/contract/test_postgres_sink.py -v
```

## Test Categories

### ScyllaDB CDC Source Connector Tests (9 tests)
- ✅ `test_connector_produces_avro_messages` - Verifies Avro serialization
- ✅ `test_connector_includes_metadata_headers` - Checks message headers
- ✅ `test_topic_naming_convention` - Validates topic naming
- ✅ `test_schema_registry_integration` - Confirms Schema Registry registration
- ✅ `test_message_key_structure` - Verifies primary key in message keys
- ✅ `test_connector_handles_all_operations` - Tests INSERT/UPDATE/DELETE capture
- ✅ `test_connector_heartbeat_messages` - Validates heartbeat mechanism
- ✅ `test_partition_assignment` - Checks topic partitioning

### PostgreSQL Sink Connector Tests (9 tests)
- ✅ `test_sink_creates_table_structure` - Verifies auto-created table schema
- ✅ `test_sink_handles_upsert_operations` - Tests INSERT and UPDATE (UPSERT)
- ✅ `test_sink_preserves_data_types` - Validates data type mappings
- ✅ `test_sink_handles_null_values` - Tests NULL value handling
- ✅ `test_sink_batch_processing` - Verifies batch message processing
- ✅ `test_sink_connector_configuration` - Validates connector settings
- ✅ `test_sink_handles_special_characters` - Tests special character preservation
- ✅ `test_sink_referential_integrity` - Validates joins across tables
- ✅ `test_sink_error_handling_configuration` - Checks error handling setup
- ✅ `test_sink_connection_pooling` - Verifies connection pool configuration

## Important Notes

### Why Cleanup is Required

The CDC pipeline processes changes continuously. Without cleanup:
- Old test data accumulates in Kafka topics and PostgreSQL
- Kafka consumer lag builds up (hundreds of pending messages)
- Tests become flaky due to timing issues
- Data from previous test runs interferes with new tests

### Automatic Cleanup

Tests include a `conftest.py` with session-scoped fixtures that automatically clean up before running:
- Runs once at the start of each test session
- Ensures all tests start with a clean slate
- Can be disabled by removing the `autouse=True` parameter

### Manual Cleanup

If tests fail or you need to reset manually:
```bash
./scripts/cleanup-test-env.sh
```

### Test Timing

Contract tests include wait times to allow for end-to-end data flow:
- CDC log processing: ~5-10 seconds
- Kafka message delivery: ~2-5 seconds
- PostgreSQL sink processing: ~10-20 seconds
- **Total end-to-end latency: 30-40 seconds**

Tests use `time.sleep(30)` after insertions to accommodate this latency.

## Troubleshooting

### Tests Failing with "assert result is not None"

**Cause**: Data hasn't flowed through the pipeline yet.

**Solutions**:
1. Run cleanup script before tests
2. Check connector status: `curl http://localhost:8083/connectors/postgres-jdbc-sink/status`
3. Verify Kafka lag: `docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-postgres-jdbc-sink --describe`

### ScyllaDB CDC Not Capturing Changes

**Cause**: CDC connector needs restart to pick up new CDC generation timestamps.

**Solution**:
```bash
curl -X POST http://localhost:8083/connectors/scylla-cdc-source/restart
```

### High Kafka Consumer Lag

**Cause**: PostgreSQL sink connector falling behind.

**Solutions**:
1. Run cleanup to reset offsets
2. Check connector logs: `docker logs kafka-connect`
3. Verify PostgreSQL is healthy: `docker exec postgres pg_isready`

### All Tests Skipped

**Cause**: Connector detection check failing.

**Solution**:
```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-jdbc-sink/status

# Restart if needed
./scripts/deploy-connectors.sh
```

## Performance Optimization

The connector configuration has been optimized for test performance:

**PostgreSQL Sink Connector**:
- `batch.size`: 3000 (process up to 3000 messages per batch)
- `poll.interval.ms`: 50 (poll for new messages every 50ms)
- `retry.backoff.ms`: 1000 (1 second backoff on errors)

These settings balance throughput and latency for testing.

## Integration with CI/CD

For CI/CD pipelines, ensure cleanup runs before tests:

```bash
#!/bin/bash
set -e

# Start services
docker-compose up -d
sleep 30  # Wait for services to be ready

# Clean environment
./scripts/cleanup-test-env.sh

# Run tests
.venv/bin/pytest tests/contract/ -v --junit-xml=test-results.xml

# Cleanup (optional)
docker-compose down -v
```

## Column Naming Convention

Due to the Debezium `ExtractNewRecordState` and `Flatten` transforms, PostgreSQL columns have a `_value` suffix:

**ScyllaDB Column** → **PostgreSQL Column**
- `username` → `username_value`
- `email` → `email_value`
- `status` → `status_value`
- `price` → `price_value`

Tests query columns using the `_value` suffix and alias them back:
```sql
SELECT status_value as status FROM cdc_data.users WHERE user_id = %s
```

## Support

For issues or questions:
1. Check connector logs: `docker logs kafka-connect`
2. Verify services: `docker ps`
3. Review this guide's troubleshooting section
4. Run cleanup and try again