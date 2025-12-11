# Testing Guide - ScyllaDB PostgreSQL CDC Pipeline

## Overview

This project includes comprehensive test suites to validate the CDC (Change Data Capture) pipeline from ScyllaDB to PostgreSQL via Kafka Connect.

## Test Categories

### 1. Contract Tests (`tests/contract/`)

Validate integration contracts between system components:
- ScyllaDB CDC Source Connector behavior
- PostgreSQL JDBC Sink Connector behavior
- Message formats and schemas
- Data type mappings

**Total: 18 tests**

See [CONTRACT_TESTS.md](tests/CONTRACT_TESTS.md) for detailed documentation.

### 2. Integration Tests (`tests/integration/`)

End-to-end validation of the complete pipeline.

### 3. Unit Tests (`tests/unit/`)

Component-level tests for individual utilities and modules.

## Quick Start

### Prerequisites

1. **Start all Docker services**:
   ```bash
   docker-compose up -d
   ```

2. **Verify services are healthy**:
   ```bash
   docker ps
   # All containers should show "healthy" or "Up"
   ```

3. **Deploy connectors** (if not already deployed):
   ```bash
   ./scripts/deploy-connectors.sh
   ```

### Running Tests

#### Contract Tests (Recommended Workflow)

**Step 1: Clean environment**
```bash
./scripts/cleanup-test-env.sh
```

**Step 2: Run tests**
```bash
.venv/bin/pytest tests/contract/ -v
```

#### All Tests
```bash
.venv/bin/pytest -v
```

#### Specific Test Categories
```bash
# Contract tests only
.venv/bin/pytest tests/contract/ -v

# Integration tests only
.venv/bin/pytest tests/integration/ -v

# Unit tests only
.venv/bin/pytest tests/unit/ -v
```

## Test Infrastructure

### Automatic Cleanup (`tests/conftest.py`)

The `conftest.py` file provides session-scoped fixtures that automatically:
- Clean PostgreSQL CDC tables before tests
- Truncate ScyllaDB tables before tests
- Reset Kafka connector state
- Wait for system stabilization

This ensures tests start with a clean state.

### Manual Cleanup (`scripts/cleanup-test-env.sh`)

Standalone script for manual environment reset:

```bash
./scripts/cleanup-test-env.sh
```

**What it does**:
1. Truncates all PostgreSQL `cdc_data.*` tables
2. Truncates all ScyllaDB `app_data.*` tables
3. Restarts ScyllaDB CDC source connector
4. Restarts PostgreSQL JDBC sink connector
5. Waits for Kafka lag to stabilize
6. Verifies cleanup success

**When to use**:
- Before running contract tests
- After test failures
- When debugging CDC issues
- Before CI/CD pipeline runs

## Test Configuration

### Wait Times

Contract tests include strategic wait times to accommodate end-to-end latency:

| Stage | Wait Time | Purpose |
|-------|-----------|---------|
| After INSERT | 30s | Allow CDC capture + Kafka delivery + sink processing |
| After UPDATE | 30s | Same as above |
| After batch operations | 30s | Process multiple messages |
| Connector restart | 5-10s | Connector initialization |

### Connector Performance Settings

Optimized for test throughput (`postgres-sink.json`):

```json
{
  "batch.size": 3000,
  "poll.interval.ms": 50,
  "retry.backoff.ms": 1000
}
```

### Column Naming Convention

Due to Kafka Connect transforms, PostgreSQL columns have `_value` suffix:

- `username` → `username_value`
- `email` → `email_value`
- `status` → `status_value`

Tests handle this automatically by aliasing columns in SQL queries.

## Common Issues and Solutions

### Issue: Tests fail with "assert result is not None"

**Cause**: Data hasn't flowed through the CDC pipeline yet.

**Solution**:
```bash
# Clean environment and retry
./scripts/cleanup-test-env.sh
.venv/bin/pytest tests/contract/ -v
```

### Issue: High Kafka consumer lag

**Symptom**: Tests timeout waiting for data.

**Diagnosis**:
```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-jdbc-sink \
  --describe
```

**Solution**: Run cleanup script to reset offsets.

### Issue: CDC not capturing new changes

**Symptom**: Inserts to ScyllaDB don't appear in Kafka.

**Solution**:
```bash
# Restart ScyllaDB CDC connector
curl -X POST http://localhost:8083/connectors/scylla-cdc-source/restart
```

### Issue: All tests skipped

**Cause**: Connector detection check failing.

**Solution**:
```bash
# Check connector status
curl http://localhost:8083/connectors/postgres-jdbc-sink/status

# Redeploy if needed
./scripts/deploy-connectors.sh
```

## CI/CD Integration

### Example GitHub Actions Workflow

```yaml
name: Contract Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Start Docker services
        run: docker-compose up -d

      - name: Wait for services
        run: sleep 30

      - name: Deploy connectors
        run: ./scripts/deploy-connectors.sh

      - name: Clean test environment
        run: ./scripts/cleanup-test-env.sh

      - name: Run contract tests
        run: .venv/bin/pytest tests/contract/ -v --junit-xml=results.xml

      - name: Publish test results
        uses: EnricoMi/publish-unit-test-result-action@v1
        if: always()
        with:
          files: results.xml

      - name: Cleanup
        if: always()
        run: docker-compose down -v
```

### Example GitLab CI

```yaml
contract_tests:
  stage: test
  services:
    - docker:dind
  before_script:
    - docker-compose up -d
    - sleep 30
    - ./scripts/deploy-connectors.sh
    - ./scripts/cleanup-test-env.sh
  script:
    - .venv/bin/pytest tests/contract/ -v --junit-xml=report.xml
  after_script:
    - docker-compose down -v
  artifacts:
    reports:
      junit: report.xml
```

## Test Coverage

Generate coverage reports:

```bash
.venv/bin/pytest --cov=src --cov-report=html --cov-report=term
```

View HTML report:
```bash
open coverage_html/index.html
```

## Test Data

### ScyllaDB Test Schema

Tables in `app_data` keyspace:
- `users`
- `products`
- `orders`
- `order_items`
- `inventory_transactions`

### PostgreSQL Test Schema

Tables auto-created in `cdc_data` schema:
- `users`
- `products`
- `orders`
- `order_items`
- `inventory_transactions`

All tests use unique UUIDs to avoid data conflicts.

## Debugging

### View Connector Logs

```bash
docker logs kafka-connect --tail=100 -f
```

### Check ScyllaDB CDC Status

```bash
docker exec scylla cqlsh -e "DESCRIBE TABLE app_data.users;"
# Look for: cdc = {'enabled': 'true', ...}
```

### Inspect Kafka Topics

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check topic content
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic scylla-cluster.app_data.users \
  --from-beginning \
  --max-messages 10
```

### Query PostgreSQL Directly

```bash
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT user_id, username_value, status_value
  FROM cdc_data.users
  LIMIT 10;
"
```

## Best Practices

1. **Always clean before contract tests**: Run `./scripts/cleanup-test-env.sh`
2. **Use unique test data**: Tests already use UUIDs for isolation
3. **Check connector health first**: Verify connectors are RUNNING before testing
4. **Monitor Kafka lag**: High lag indicates sink performance issues
5. **Review logs on failure**: Connector logs often reveal root causes
6. **Don't skip cleanup**: Accumulated data causes flaky tests

## Support

For detailed contract test documentation, see [CONTRACT_TESTS.md](tests/CONTRACT_TESTS.md).

For connector deployment, see [deploy-connectors.sh](scripts/deploy-connectors.sh).

For cleanup details, see [cleanup-test-env.sh](scripts/cleanup-test-env.sh).