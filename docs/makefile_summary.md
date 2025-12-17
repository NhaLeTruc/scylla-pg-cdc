# Makefile Created for ScyllaDB-Postgres CDC Pipeline

## Overview

Created a comprehensive Makefile with **40+ commands** organized into logical sections for quick start, testing, monitoring, and development.

## Key Features

### 1. **Fresh-Start Testing** (Solves Your Issue!)

All testing commands automatically use `cleanup-full` for a fresh start:

```bash
make test              # All tests with fresh-start
make test-contract     # Contract tests with fresh-start  
make test-integration  # Integration tests with fresh-start
```

These commands run: `./scripts/cleanup-test-env.sh --full`
- Truncates data
- Resets Kafka consumer offsets
- Recreates connectors
- **Ensures no stuck offset issues!**

### 2. **Quick Start Commands**

```bash
make setup              # Complete environment setup
make deploy-connectors  # Deploy CDC connectors
make test-replication   # Quick CDC test (INSERT/UPDATE/DELETE)
make cleanup-full       # Fresh-start cleanup
```

### 3. **Cleanup Options** (Three Modes!)

```bash
make cleanup            # Standard: truncate + restart
make cleanup-full       # Full fresh-start (recommended for testing)
make cleanup-connectors # Quick: only restart connectors
```

### 4. **Development Workflow**

```bash
make install-deps       # Setup Python environment
make fmt                # Format code with black
make lint               # Lint with ruff
make test-unit          # Fast unit tests
```

### 5. **Monitoring & Health**

```bash
make health             # Check all services
make monitor            # Monitor CDC connectors
make check-replication  # Verify CDC data completeness
make logs-kafka-connect # Tail connector logs
```

### 6. **Database Access**

```bash
make shell-scylla       # CQL shell
make shell-postgres     # psql shell
make count-scylla       # Show ScyllaDB row counts
make count-postgres     # Show PostgreSQL row counts
make query-postgres-users  # Quick user query
```

### 7. **CI/CD Support**

```bash
make ci-setup           # Setup for CI
make ci-test            # Run all tests
make ci-teardown        # Cleanup
```

## Usage Examples

### First Time Setup
```bash
make setup
make deploy-connectors
make test-replication
```

### Run Tests with Fresh Environment
```bash
make test-contract      # Runs cleanup-full automatically!
```

### Debug CDC Pipeline
```bash
make cleanup-full       # Fresh start
make deploy-connectors  # Redeploy
make monitor            # Check status
make logs-kafka-connect # View logs
```

### Daily Development
```bash
make cleanup            # Quick cleanup
make test-unit          # Fast unit tests
make check-replication  # Verify CDC working
```

## Help System

Run `make` or `make help` to see all commands with descriptions organized by category!

## Integration with README.md

The Makefile commands align with README.md sections:
- **Quick Start** section → `make setup`, `make start`
- **Testing** section → `make test`, `make test-contract`
- **Live Test Data Replication** → `make test-replication`, `make check-replication`

Users can now use simple commands instead of long script paths!
