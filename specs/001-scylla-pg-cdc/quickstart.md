# Quickstart Guide: ScyllaDB to Postgres CDC Pipeline

**Last Updated**: 2025-12-09
**Est. Setup Time**: 15-20 minutes
**Prerequisites**: Docker 24+, Docker Compose 2.23+, 8GB RAM, 20GB disk space

## Table of Contents

1. [Quick Start (5 minutes)](#quick-start)
2. [Local Development Setup](#local-development-setup)
3. [Deploy Your First Connector](#deploy-your-first-connector)
4. [Verify Data Replication](#verify-data-replication)
5. [Monitoring & Observability](#monitoring--observability)
6. [Troubleshooting](#troubleshooting)
7. [Next Steps](#next-steps)

## Quick Start

Get the CDC pipeline running in 5 commands:

```bash
# 1. Clone repository
git clone <repository-url>
cd scylla-pg-cdc

# 2. Copy environment template
cp .env.example .env

# 3. Start all services
docker-compose up -d

# 4. Wait for services to be healthy (2-3 minutes)
./scripts/health-check.sh --wait

# 5. Deploy example connector
./scripts/deploy-connector.sh --template examples/users-table
```

**Verification**:
```bash
# Insert test data into ScyllaDB
docker exec -it scylla cqlsh -e "
  INSERT INTO ecommerce.users (user_id, email, name)
  VALUES (1, 'alice@example.com', 'Alice Smith');
"

# Check Postgres received the data (wait ~5 seconds)
docker exec -it postgres psql -U postgres -d warehouse -c "
  SELECT * FROM ecommerce.users WHERE user_id = 1;
"
```

If you see Alice's data in Postgres, **you're done**! The CDC pipeline is working.

---

## Local Development Setup

### System Requirements

**Minimum**:
- CPU: 4 cores
- RAM: 8GB
- Disk: 20GB free space
- OS: Linux, macOS, or Windows with WSL2

**Recommended**:
- CPU: 8+ cores
- RAM: 16GB
- Disk: 50GB free space (for logs and data retention)

### Prerequisites Installation

<details>
<summary><b>Docker & Docker Compose</b></summary>

**Linux**:
```bash
# Docker Engine
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.23.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

**macOS**:
```bash
brew install docker docker-compose
```

**Windows** (WSL2):
- Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)
- Enable WSL2 backend in Docker Desktop settings
</details>

<details>
<summary><b>Python 3.11+ (for reconciliation scripts)</b></summary>

```bash
# Install Python and venv
sudo apt-get install python3.11 python3.11-venv python3-pip  # Linux
brew install python@3.11  # macOS

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```
</details>

### Environment Configuration

1. **Copy environment template**:
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` file** (optional for local dev, defaults work out-of-the-box):
   ```bash
   # ScyllaDB Configuration
   SCYLLA_HOSTS=scylla:9042
   SCYLLA_KEYSPACE=ecommerce
   SCYLLA_REPLICATION=1  # Use 3 in production

   # Postgres Configuration
   POSTGRES_HOST=postgres
   POSTGRES_PORT=5432
   POSTGRES_DB=warehouse
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=postgres  # Change in production!

   # Kafka Configuration
   KAFKA_BROKERS=kafka:9092
   SCHEMA_REGISTRY_URL=http://schema-registry:8081

   # Vault Configuration
   VAULT_ADDR=http://vault:8200
   VAULT_TOKEN=dev-token  # Auto-generated in dev mode

   # Monitoring
   PROMETHEUS_PORT=9090
   GRAFANA_PORT=3000
   JAEGER_UI_PORT=16686
   ```

3. **Initialize Vault secrets** (dev mode auto-initializes):
   ```bash
   # Vault starts in dev mode with root token 'dev-token'
   # Secrets are auto-populated by docker/vault/init-secrets.sh
   # No manual action required for local development
   ```

### Start Services

**Full Stack** (with monitoring):
```bash
docker-compose --profile monitoring up -d
```

**Core Pipeline Only** (faster startup, no Grafana/Prometheus/Jaeger):
```bash
docker-compose up -d
```

**Check Service Health**:
```bash
./scripts/health-check.sh
```

Expected output:
```
✓ Zookeeper: healthy
✓ Kafka: healthy
✓ Schema Registry: healthy
✓ Kafka Connect: healthy (3 plugins loaded)
✓ ScyllaDB: healthy (1 node up)
✓ Postgres: healthy (accepting connections)
✓ Vault: healthy (unsealed)
✓ Prometheus: healthy (optional, --profile monitoring)
✓ Grafana: healthy (optional, --profile monitoring)
✓ Jaeger: healthy (optional, --profile monitoring)

All services ready! 🚀
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka Connect API | http://localhost:8083 | None |
| Schema Registry | http://localhost:8081 | None |
| Vault UI | http://localhost:8200/ui | Token: `dev-token` |
| Prometheus | http://localhost:9090 | None |
| Grafana | http://localhost:3000 | admin / admin |
| Jaeger UI | http://localhost:16686 | None |
| ScyllaDB (CQL) | localhost:9042 | None (dev mode) |
| Postgres | localhost:5432 | postgres / postgres |

---

## Deploy Your First Connector

### Step 1: Create Source Table in ScyllaDB

```bash
# Connect to ScyllaDB
docker exec -it scylla cqlsh

# Create keyspace (if not exists)
CREATE KEYSPACE IF NOT EXISTS ecommerce
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

# Create source table with CDC enabled
CREATE TABLE ecommerce.users (
    user_id BIGINT PRIMARY KEY,
    email TEXT,
    name TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
) WITH cdc = {'enabled': true};

# Insert sample data
INSERT INTO ecommerce.users (user_id, email, name, created_at, updated_at)
VALUES (1, 'alice@example.com', 'Alice Smith', toTimestamp(now()), toTimestamp(now()));

INSERT INTO ecommerce.users (user_id, email, name, created_at, updated_at)
VALUES (2, 'bob@example.com', 'Bob Jones', toTimestamp(now()), toTimestamp(now()));

# Verify CDC log table was auto-created
DESCRIBE TABLE ecommerce.users_scylla_cdc_log;

# Exit
exit;
```

### Step 2: Deploy Scylla Source Connector

```bash
# Use deployment script
./scripts/deploy-connector.sh \
  --name scylla-source-users \
  --type source \
  --table ecommerce.users \
  --tasks 2

# Verify connector status
curl http://localhost:8083/connectors/scylla-source-users/status | jq
```

Expected response:
```json
{
  "name": "scylla-source-users",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {"id": 0, "state": "RUNNING", "worker_id": "kafka-connect:8083"},
    {"id": 1, "state": "RUNNING", "worker_id": "kafka-connect:8083"}
  ],
  "type": "source"
}
```

### Step 3: Verify Kafka Topic

```bash
# List topics (should see ecommerce.users)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep ecommerce

# Consume messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce.users \
  --from-beginning \
  --max-messages 2

# Check schema registry
curl http://localhost:8081/subjects/ecommerce.users-value/versions/latest | jq '.schema' | jq
```

### Step 4: Deploy Postgres Sink Connector

```bash
# Deploy sink connector
./scripts/deploy-connector.sh \
  --name postgres-sink-users \
  --type sink \
  --topic ecommerce.users \
  --table ecommerce.users \
  --tasks 2

# Verify connector status
curl http://localhost:8083/connectors/postgres-sink-users/status | jq
```

### Step 5: Verify Target Table in Postgres

```bash
# Check table was auto-created
docker exec postgres psql -U postgres -d warehouse -c "\d ecommerce.users"

# Query replicated data
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT user_id, email, name FROM ecommerce.users ORDER BY user_id;
"
```

Expected output:
```
 user_id |       email        |    name
---------+--------------------+-------------
       1 | alice@example.com  | Alice Smith
       2 | bob@example.com    | Bob Jones
(2 rows)
```

**Congratulations!** Your CDC pipeline is live. Changes to `ecommerce.users` in ScyllaDB will now automatically flow to Postgres.

---

## Verify Data Replication

### Test INSERT

```bash
# Insert in ScyllaDB
docker exec scylla cqlsh -e "
  INSERT INTO ecommerce.users (user_id, email, name, created_at, updated_at)
  VALUES (3, 'charlie@example.com', 'Charlie Brown', toTimestamp(now()), toTimestamp(now()));
"

# Wait 2-5 seconds for pipeline latency

# Verify in Postgres
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT * FROM ecommerce.users WHERE user_id = 3;
"
```

### Test UPDATE

```bash
# Update in ScyllaDB
docker exec scylla cqlsh -e "
  UPDATE ecommerce.users
  SET name = 'Alice Johnson', updated_at = toTimestamp(now())
  WHERE user_id = 1;
"

# Verify in Postgres
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT user_id, name FROM ecommerce.users WHERE user_id = 1;
"
```

### Test DELETE

```bash
# Delete in ScyllaDB
docker exec scylla cqlsh -e "
  DELETE FROM ecommerce.users WHERE user_id = 2;
"

# Verify in Postgres (row should be gone)
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT * FROM ecommerce.users WHERE user_id = 2;
"
```

### Measure End-to-End Latency

```bash
# Script to measure latency
./scripts/measure-latency.sh --table ecommerce.users --samples 10
```

Expected output:
```
Measuring CDC pipeline latency for ecommerce.users (10 samples)...

Sample 1: 2.3 seconds
Sample 2: 1.8 seconds
Sample 3: 2.1 seconds
...
Sample 10: 2.0 seconds

Average latency: 2.05 seconds
p50: 2.0 seconds
p95: 2.4 seconds
p99: 2.8 seconds

✓ Within SLA (<5 seconds p95)
```

---

## Monitoring & Observability

### Access Dashboards

**Grafana** (http://localhost:3000):
1. Login: admin / admin
2. Navigate to **Dashboards** → **CDC Pipeline Overview**
3. View:
   - Connector lag (time behind source)
   - Throughput (events/sec)
   - Error rates
   - Dead letter queue size

**Prometheus** (http://localhost:9090):
- Query metrics directly:
  ```promql
  # Connector lag
  kafka_connect_sink_connector_lag_seconds{connector="postgres-sink-users"}

  # Throughput
  rate(kafka_topics_messages_in_total{topic="ecommerce.users"}[5m])

  # Error rate
  rate(kafka_connect_connector_task_error_total[5m])
  ```

**Jaeger** (http://localhost:16686):
1. Select service: `kafka-connect`
2. Search for recent traces
3. Click trace to see end-to-end flow: ScyllaDB → Kafka → Postgres

### Key Metrics to Monitor

| Metric | Query | Alert Threshold |
|--------|-------|----------------|
| Connector Lag | `kafka_connect_sink_connector_lag_seconds` | > 10 seconds |
| Throughput | `rate(kafka_topics_messages_in_total[5m])` | < expected rate |
| Error Rate | `rate(kafka_connect_connector_task_error_total[5m])` | > 0 |
| DLQ Messages | `kafka_topics_messages_in_total{topic=~"dlq-.*"}` | > 0 |
| Connector Status | `kafka_connect_connector_status` | != 1 (RUNNING) |

### View Logs

```bash
# Kafka Connect logs
docker logs kafka-connect -f --tail 100

# Filter for errors
docker logs kafka-connect 2>&1 | grep ERROR

# Structured JSON logs with correlation IDs
docker logs kafka-connect 2>&1 | jq 'select(.level == "ERROR")'

# All services logs
docker-compose logs -f
```

---

## Troubleshooting

### Connector Won't Start

**Symptom**: Connector status shows `FAILED`

**Check**:
```bash
curl http://localhost:8083/connectors/scylla-source-users/status | jq '.connector.trace'
```

**Common Causes**:
1. **Invalid credentials**: Check Vault secrets
   ```bash
   docker exec vault vault kv get cdc/scylla
   ```

2. **ScyllaDB not ready**: Verify ScyllaDB health
   ```bash
   docker exec scylla nodetool status
   ```

3. **Missing CDC log table**: Ensure table created with `cdc = {'enabled': true}`
   ```bash
   docker exec scylla cqlsh -e "DESCRIBE TABLE ecommerce.users;"
   ```

**Fix**:
```bash
# Restart connector
./scripts/deploy-connector.sh --name scylla-source-users --restart
```

### Schema Evolution Error

**Symptom**: DLQ topic has messages

**Check DLQ**:
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq-scylla-source \
  --from-beginning \
  --max-messages 1 | jq
```

**Common Causes**:
1. **Incompatible schema change**: Type change (INT → STRING)
2. **Missing default value**: New required column without default

**Fix**:
1. Pause connector:
   ```bash
   curl -X PUT http://localhost:8083/connectors/scylla-source-users/pause
   ```

2. Fix schema issue in ScyllaDB

3. Clear DLQ (if messages are invalid):
   ```bash
   docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
     --delete --topic dlq-scylla-source
   ```

4. Resume connector:
   ```bash
   curl -X PUT http://localhost:8083/connectors/scylla-source-users/resume
   ```

### High Lag

**Symptom**: Lag > 10 seconds

**Diagnose**:
```bash
# Check Kafka Connect resource usage
docker stats kafka-connect

# Check Kafka broker health
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Postgres connection pool
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT count(*) FROM pg_stat_activity WHERE datname = 'warehouse';
"
```

**Fixes**:
1. **Increase connector tasks**: More parallelism
   ```bash
   curl -X PUT http://localhost:8083/connectors/postgres-sink-users/config \
     -H "Content-Type: application/json" \
     -d '{"tasks.max": "4"}'
   ```

2. **Increase batch size**: Higher throughput
   ```bash
   # Edit connector config
   {
     "batch.size": "5000",  # Increase from 3000
     "linger.ms": "200"     # Allow more batching
   }
   ```

3. **Scale Kafka Connect workers**: Add more worker nodes

### Reconciliation Discrepancies

**Symptom**: Reconciliation finds missing or mismatched rows

**Run Reconciliation**:
```bash
./scripts/reconcile.py \
  --table ecommerce.users \
  --mode full \
  --repair \
  --log-json
```

**Check Results**:
```bash
# Query discrepancies
docker exec postgres psql -U postgres -d warehouse -c "
  SELECT discrepancy_type, COUNT(*)
  FROM cdc_metadata.reconciliation_log
  WHERE table_name = 'ecommerce.users' AND NOT resolved
  GROUP BY discrepancy_type;
"
```

**Common Causes**:
1. **Connector downtime**: Events missed during outage
2. **Poison messages in DLQ**: Failed events not retried
3. **Manual database changes**: Direct writes bypassing CDC

**Prevention**:
- Schedule regular reconciliation (daily cron job)
- Monitor DLQ topic for non-zero messages
- Avoid direct writes to Postgres (use ScyllaDB as source of truth)

---

## Next Steps

### Production Readiness Checklist

- [ ] **Change default passwords** (Postgres, Vault, Grafana)
- [ ] **Enable TLS** for Kafka, ScyllaDB, Postgres connections
- [ ] **Configure Vault production mode** (unsealed, HA backend)
- [ ] **Set up Kafka replication** (replication factor 3)
- [ ] **Configure retention policies** (Kafka 7+ days, logs 30 days)
- [ ] **Set up alerting** (Prometheus AlertManager rules)
- [ ] **Deploy monitoring** (dedicated Prometheus/Grafana instances)
- [ ] **Configure backups** (Postgres, Kafka offsets, Vault keys)
- [ ] **Set up log aggregation** (ELK stack or similar)
- [ ] **Document runbook procedures** (failure recovery, scaling)
- [ ] **Load test pipeline** (verify throughput/latency SLAs)
- [ ] **Schedule reconciliation** (daily cron job)

### Learn More

- **Architecture**: [docs/architecture.md](../../docs/architecture.md)
- **Runbook**: [docs/runbook.md](../../docs/runbook.md)
- **Scaling Guide**: [docs/scaling.md](../../docs/scaling.md)
- **Troubleshooting**: [docs/troubleshooting.md](../../docs/troubleshooting.md)
- **Research**: [research.md](research.md)
- **Data Model**: [data-model.md](data-model.md)
- **API Contracts**: [contracts/](contracts/)

### Add More Tables

1. Create table in ScyllaDB with CDC enabled
2. Deploy source connector:
   ```bash
   ./scripts/deploy-connector.sh --name scylla-source-<table> --table <keyspace>.<table>
   ```
3. Deploy sink connector:
   ```bash
   ./scripts/deploy-connector.sh --name postgres-sink-<table> --topic <keyspace>.<table>
   ```

### Customize Connectors

Edit connector templates in `configs/connectors/`:
- `scylla-source.json.template` - Source connector config
- `postgres-sink.json.template` - Sink connector config

Key tuning parameters:
- `tasks.max`: Parallelism (2-8 per connector)
- `batch.size`: Throughput vs latency (1000-5000)
- `max.retries`: Retry attempts (5-20)
- `retry.backoff.ms`: Backoff time (1000-5000ms)

### Community & Support

- **Report Issues**: [GitHub Issues](https://github.com/<org>/<repo>/issues)
- **Discussions**: [GitHub Discussions](https://github.com/<org>/<repo>/discussions)
- **Slack**: [#cdc-pipeline](https://slack.example.com)
- **Documentation**: [https://docs.example.com/cdc](https://docs.example.com/cdc)
