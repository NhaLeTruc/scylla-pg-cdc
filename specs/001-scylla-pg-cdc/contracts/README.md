# Contracts Directory

This directory contains API and CLI contracts for the CDC pipeline infrastructure.

## Contents

### 1. kafka-connect-api.yaml

**Type**: OpenAPI 3.0 REST API specification
**Purpose**: Kafka Connect REST API contract for connector management
**Used By**:
- `scripts/deploy-connector.sh` - Deploy/update connectors
- `scripts/health-check.sh` - Monitor connector health
- Contract tests (`tests/contract/test_kafka_connect_api.py`)

**Key Endpoints**:
- `POST /connectors` - Deploy new connector
- `GET /connectors/{name}/status` - Check health
- `PUT /connectors/{name}/pause` - Pause connector
- `POST /connectors/{name}/restart` - Restart connector

**Testing**:
```bash
# Validate contract
npm install -g @stoplight/spectral-cli
spectral lint contracts/kafka-connect-api.yaml

# Run contract tests
pytest tests/contract/test_kafka_connect_api.py
```

### 2. reconciliation-cli.yaml

**Type**: OpenAPI 3.0 CLI specification (non-HTTP)
**Purpose**: Command-line interface contract for reconciliation script
**Used By**:
- `scripts/reconcile.py` - Main reconciliation script
- `scripts/schedule-reconciliation.sh` - Cron wrapper
- Contract tests (`tests/contract/test_reconciliation_cli.py`)

**Key Commands**:
- `reconcile.py --table <name> --mode full --repair` - Full reconciliation
- `reconcile.py --table <name> --mode incremental --since <date>` - Incremental
- `reconcile.py status` - Check running reconciliations
- `reconcile.py report --days 7` - Generate report

**Exit Codes**:
- `0` - Success (no discrepancies)
- `1` - Discrepancies found and repaired
- `2` - Repair failures
- `10` - Configuration error
- `11` - Connection error
- `130` - User interrupted (SIGINT)

**Testing**:
```bash
# Run CLI contract tests
pytest tests/contract/test_reconciliation_cli.py

# Manual testing
./scripts/reconcile.py --help
./scripts/reconcile.py --table test.users --mode verify-only --dry-run
```

## Contract Testing Strategy

### Why Contract Tests?

Contract tests verify that:
1. **API Producers** (Kafka Connect) conform to expected interface
2. **API Consumers** (our scripts) handle responses correctly
3. **CLI Tools** provide stable interfaces for automation
4. **Breaking changes** are detected early

### Test Approach

**For REST APIs** (kafka-connect-api.yaml):
- Use [Prism](https://stoplight.io/open-source/prism/) or [WireMock](http://wiremock.org/) to mock Kafka Connect API
- Validate scripts make correct API calls with expected payloads
- Verify scripts handle error responses gracefully

**For CLI Tools** (reconciliation-cli.yaml):
- Invoke CLI with various argument combinations
- Assert exit codes match specification
- Validate JSON output structure
- Test error handling for invalid inputs

### Running Contract Tests

```bash
# Install dependencies
pip install -r requirements-test.txt

# Run all contract tests
pytest tests/contract/ -v

# Run with contract validation
pytest tests/contract/ -v --validate-contracts

# Generate contract test report
pytest tests/contract/ --cov=src --cov-report=html
```

## Versioning

Contracts follow [Semantic Versioning](https://semver.org/):
- **MAJOR**: Breaking changes to API/CLI (incompatible with previous version)
- **MINOR**: Backward-compatible additions (new optional parameters)
- **PATCH**: Bug fixes, documentation improvements

Current Versions:
- kafka-connect-api.yaml: 3.6.1 (tracks Kafka Connect version)
- reconciliation-cli.yaml: 1.0.0

## References

- [OpenAPI 3.0 Specification](https://swagger.io/specification/)
- [Kafka Connect REST API Documentation](https://docs.confluent.io/platform/current/connect/references/restapi.html)
- [Contract Testing Patterns](https://martinfowler.com/bliki/ContractTest.html)
