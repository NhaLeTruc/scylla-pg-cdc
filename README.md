# ScyllaDB to Postgres CDC Pipeline

Production-ready change data capture (CDC) pipeline that streams real-time change events from ScyllaDB to a Postgres data warehouse.

## Features

- **Exactly-Once Semantics**: No data loss or duplication using Kafka transactions
- **Automatic Schema Evolution**: Zero-downtime schema changes with backward compatibility
- **Full Observability**: Prometheus metrics, Grafana dashboards, Jaeger distributed tracing
- **Failure Recovery**: Automatic retry with exponential backoff, dead letter queues, reconciliation
- **Locally Testable**: Complete Docker Compose environment for development
- **CLI-Managed**: Command-line scripts, no centralized management API needed
- **Secure**: HashiCorp Vault credential management, TLS encryption, SQL injection protection

## Architecture

```
ScyllaDB (Source)
    ↓ CDC logs
Scylla CDC Source Connector
    ↓ Kafka topics (Avro)
Kafka Connect Framework
    ↓ Schema Registry validation
Postgres JDBC Sink Connector
    ↓ UPSERT operations
PostgreSQL (Data Warehouse)
```

## Quick Start

### Prerequisites

- Docker 24+ and Docker Compose 2.23+
- Python 3.11+ (for reconciliation scripts)
- 8GB RAM, 20GB disk space

### 5-Minute Setup

```bash
# 1. Clone and navigate
git clone <repository-url>
cd scylla-pg-cdc

# 2. Copy environment template
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Wait for services (2-3 minutes)
./scripts/health-check.sh --wait

# 5. Deploy example connector
./scripts/deploy-connector.sh --template examples/users-table
```

### Verify Replication

```bash
# Insert test data in ScyllaDB
docker exec -it scylla cqlsh -e "
  INSERT INTO ecommerce.users (user_id, email, name)
  VALUES (1, 'alice@example.com', 'Alice Smith');
"

# Check Postgres received the data (~5 seconds)
docker exec -it postgres psql -U postgres -d warehouse -c "
  SELECT * FROM ecommerce.users WHERE user_id = 1;
"
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka Connect API | http://localhost:8083 | None |
| Schema Registry | http://localhost:8081 | None |
| Vault UI | http://localhost:8200/ui | Token: `dev-token` |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | admin / admin |
| Jaeger UI | http://localhost:16686 | None |

## Documentation

- [Quickstart Guide](specs/001-scylla-pg-cdc/quickstart.md) - Detailed setup and usage
- [Architecture](docs/architecture.md) - System design and components
- [Runbook](docs/runbook.md) - Operational procedures
- [Troubleshooting](docs/troubleshooting.md) - Common issues and solutions
- [Scaling Guide](docs/scaling.md) - Capacity planning and horizontal scaling

## Project Structure

```
/
├── docker/           # Docker Compose and service configurations
├── scripts/          # Operational scripts (deploy, monitor, reconcile)
├── src/              # Custom Python code (reconciliation, monitoring, utils)
├── tests/            # Test suite (unit, integration, contract)
├── configs/          # Connector and infrastructure configurations
├── docs/             # Operational documentation
└── specs/            # Design documents and task plans
```

## Management Scripts

### Deployment

```bash
# Deploy source connector
./scripts/deploy-connector.sh --name scylla-source-users --table ecommerce.users

# Deploy sink connector
./scripts/deploy-connector.sh --name postgres-sink-users --topic ecommerce.users

# Check connector status
./scripts/health-check.sh
```

### Monitoring

```bash
# View logs
docker-compose logs -f kafka-connect

# Check dead letter queue
./scripts/check-dlq.sh

# Measure latency
./scripts/measure-latency.sh --table ecommerce.users
```

### Reconciliation

```bash
# Full reconciliation with repair
./scripts/reconcile.py --table ecommerce.users --mode full --repair

# Incremental since date
./scripts/reconcile.py --table ecommerce.orders --mode incremental --since "2025-12-08" --repair

# Verify only (no repairs)
./scripts/reconcile.py --table analytics.events --mode verify-only

# Check reconciliation status
./scripts/reconcile.py status
```

## Performance

- **Throughput**: 10,000+ events/second
- **Latency**: <5 seconds p95 end-to-end
- **Recovery**: <30 seconds from failures
- **Scale**: 100+ tables, 1TB+ daily change volume
- **Availability**: 99.9% uptime target

## Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Run unit tests only
pytest -m unit

# Run integration tests (requires Docker)
pytest -m integration

# Run all tests with coverage
pytest

# Run contract tests
pytest -m contract
```

## Development

```bash
# Create Python virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start local environment
docker-compose up -d

# Run tests
pytest

# Stop environment
docker-compose down
```

## Production Deployment

See [Production Checklist](specs/001-scylla-pg-cdc/quickstart.md#production-readiness-checklist) for:

- [ ] Change default passwords
- [ ] Enable TLS for all connections
- [ ] Configure Vault production mode
- [ ] Set up Kafka replication (factor 3)
- [ ] Configure retention policies
- [ ] Set up alerting (Prometheus AlertManager)
- [ ] Deploy monitoring stack
- [ ] Configure backups
- [ ] Set up log aggregation
- [ ] Document runbook procedures
- [ ] Load test pipeline
- [ ] Schedule reconciliation (daily cron)

## License

[Add your license here]

## Contributing

[Add contributing guidelines here]

## Support

- **Issues**: [GitHub Issues](https://github.com/<org>/<repo>/issues)
- **Discussions**: [GitHub Discussions](https://github.com/<org>/<repo>/discussions)
- **Documentation**: [Full Docs](https://docs.example.com/cdc)
