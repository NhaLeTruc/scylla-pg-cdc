# ScyllaDB to PostgreSQL CDC Pipeline - Troubleshooting Guide

## Overview

This guide provides solutions to common issues encountered when operating the CDC pipeline. For operational procedures, see [docs/runbook.md](runbook.md).

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Connector Issues](#connector-issues)
3. [Replication Issues](#replication-issues)
4. [Schema Evolution Issues](#schema-evolution-issues)
5. [Performance Issues](#performance-issues)
6. [Data Consistency Issues](#data-consistency-issues)
7. [Infrastructure Issues](#infrastructure-issues)
8. [Monitoring and Alerting Issues](#monitoring-and-alerting-issues)

---

## Quick Diagnostics

### Initial Triage

Run these commands first to identify the problem area:

```bash
# 1. Run comprehensive health check
./scripts/health-check.sh

# 2. Check for active alerts
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[] | {alert: .labels.alertname, severity: .labels.severity, state: .state}'

# 3. Check recent errors
docker-compose logs --tail=50 --since=10m | grep -i error

# 4. Check connector status
curl -s http://localhost:8083/connectors | jq '.[]' | while read connector; do
  echo "Connector: $connector"
  curl -s "http://localhost:8083/connectors/${connector}/status" | jq .
done

# 5. Check DLQ for failed messages
./scripts/check-dlq.sh
```

### Service Status Check

```bash
# Check all services are running
docker-compose ps

# Expected output: All services "Up"
# If any service is not "Up", check logs:
docker-compose logs <service-name>
```

---

## Connector Issues

### Issue 1: Connector in FAILED State

**Symptoms**:
- Connector status shows `FAILED`
- Tasks show `FAILED` state
- No data replication occurring

**Diagnosis**:

```bash
# Check connector status
curl -s http://localhost:8083/connectors/scylla-source/status | jq .

# View error trace
curl -s http://localhost:8083/connectors/scylla-source/status | jq '.tasks[].trace'

# Check connector logs
docker-compose logs kafka-connect | grep -A 20 "scylla-source.*ERROR"
```

**Common Causes and Solutions**:

#### Cause 1: Configuration Error

**Error**: `ConfigException: Invalid value for configuration`

**Solution**:
```bash
# Validate configuration
cat configs/connectors/scylla-source.json | jq .

# Common issues:
# - Invalid port numbers
# - Missing required fields
# - Incorrect connector class name

# Fix configuration and redeploy
vim configs/connectors/scylla-source.json
./scripts/deploy-connector.sh update scylla-source
```

#### Cause 2: Database Connection Failed

**Error**: `Unable to connect to database`, `Connection refused`

**Solution**:
```bash
# Test ScyllaDB connection
docker exec scylla cqlsh -e "SELECT now() FROM system.local;"

# Check network connectivity
docker exec kafka-connect nc -zv scylla 9042

# Verify credentials
docker exec kafka-connect cat /vault/secrets/scylla-credentials

# If connection fails, restart ScyllaDB
docker-compose restart scylla
sleep 30
./scripts/deploy-connector.sh restart scylla-source
```

#### Cause 3: Schema Registry Unavailable

**Error**: `Failed to register schema`, `Schema Registry connection failed`

**Solution**:
```bash
# Check Schema Registry
curl -s http://localhost:8081/ | jq .

# If not responding, restart
docker-compose restart schema-registry
sleep 30

# Restart connectors after Schema Registry is back
./scripts/deploy-connector.sh restart scylla-source
./scripts/deploy-connector.sh restart postgres-sink
```

### Issue 2: Connector Tasks Stuck in RESTARTING Loop

**Symptoms**:
- Tasks continuously restart
- Connector status alternates between RUNNING and FAILED
- High CPU usage on Kafka Connect worker

**Diagnosis**:

```bash
# Check task restart count
curl -s http://localhost:8083/connectors/scylla-source/status | \
  jq '.tasks[] | {id: .id, state: .state, worker_id: .worker_id}'

# Monitor task state changes
watch -n 2 'curl -s http://localhost:8083/connectors/scylla-source/status | jq .connector.state'

# Check for OutOfMemory errors
docker-compose logs kafka-connect | grep -i "OutOfMemoryError"
```

**Common Causes and Solutions**:

#### Cause 1: Insufficient Memory

**Solution**:
```bash
# Increase Kafka Connect heap size
vim docker-compose.yml

# Change:
# KAFKA_HEAP_OPTS: "-Xms2G -Xmx4G"
# To:
# KAFKA_HEAP_OPTS: "-Xms4G -Xmx8G"

# Restart Kafka Connect
docker-compose restart kafka-connect
sleep 60
```

#### Cause 2: Corrupted Offset

**Solution**:
```bash
# Stop connector
./scripts/deploy-connector.sh delete scylla-source

# Reset offsets (WARNING: May cause data loss or duplicates)
docker exec kafka kafka-streams-application-reset \
  --application-id connect-scylla-source \
  --bootstrap-servers localhost:9092 \
  --input-topics cdc.scylla.users

# Redeploy connector
./scripts/deploy-connector.sh create scylla-source
```

#### Cause 3: Poison Message

**Solution**:
```bash
# Check DLQ for poison messages
./scripts/check-dlq.sh

# Skip poison message by advancing offset manually
# (Advanced: Requires careful offset management)

# Or configure error tolerance
vim configs/connectors/scylla-source.json

# Set:
"errors.tolerance": "all"
"errors.deadletterqueue.topic.name": "dlq-scylla-source"

# Update connector
./scripts/deploy-connector.sh update scylla-source
```

### Issue 3: No Connector Tasks Running

**Symptoms**:
- Connector shows `RUNNING` but 0/4 tasks
- No data replication occurring

**Diagnosis**:

```bash
# Check task allocation
curl -s http://localhost:8083/connectors/scylla-source/status | jq '.tasks'

# Check worker availability
curl -s http://localhost:8083/ | jq '.
```

**Solution**:

```bash
# Check if workers are overloaded
curl -s http://localhost:8083/connectors | jq 'length'

# If too many connectors per worker, add more workers
docker-compose up -d --scale kafka-connect=6

# Wait for rebalance
sleep 60

# Verify tasks are assigned
curl -s http://localhost:8083/connectors/scylla-source/status | jq .
```

---

## Replication Issues

### Issue 4: High Replication Lag

**Symptoms**:
- `cdc_replication_lag_seconds > 300`
- Alert: "High Replication Lag"
- Increasing consumer lag in Kafka

**Diagnosis**:

```bash
# Check replication lag
curl -s 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | \
  jq '.data.result[] | {table: .metric.table, lag: .value[1]}'

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-sink

# Check sink throughput
curl -s 'http://localhost:9090/api/v1/query?query=rate(cdc_records_replicated_total[5m])' | \
  jq '.data.result[] | {table: .metric.table, rate: .value[1]}'
```

**Common Causes and Solutions**:

#### Cause 1: Insufficient Sink Connector Parallelism

**Solution**:
```bash
# Increase task count
vim configs/connectors/postgres-sink.json

# Change:
"tasks.max": "4"
# To:
"tasks.max": "8"

# Update connector
./scripts/deploy-connector.sh update postgres-sink

# Monitor lag decrease
watch -n 10 'curl -s http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds'
```

#### Cause 2: PostgreSQL Performance Bottleneck

**Solution**:
```bash
# Check PostgreSQL load
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT pid, usename, state, query FROM pg_stat_activity WHERE state != 'idle';"

# Check for long-running queries
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT pid, now() - query_start AS duration, query
   FROM pg_stat_activity
   WHERE state != 'idle'
   ORDER BY duration DESC;"

# Kill blocking queries if necessary
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT pg_terminate_backend(<pid>);"

# Optimize PostgreSQL configuration
vim configs/postgres/postgresql.conf

# Increase:
max_connections = 200        # from 100
shared_buffers = 8GB         # from 4GB
effective_cache_size = 24GB  # from 12GB

# Restart PostgreSQL
docker-compose restart postgres
```

#### Cause 3: Small Batch Size

**Solution**:
```bash
# Increase batch size
vim configs/connectors/postgres-sink.json

# Change:
"batch.size": "1000"
# To:
"batch.size": "5000"

# Update connector
./scripts/deploy-connector.sh update postgres-sink
```

### Issue 5: No Data Replication

**Symptoms**:
- Connectors showing `RUNNING`
- No records in PostgreSQL
- No messages in Kafka topics

**Diagnosis**:

```bash
# Check if CDC is enabled in ScyllaDB
docker exec scylla cqlsh -e \
  "SELECT * FROM system_distributed.cdc_streams_descriptions_v2 LIMIT 10;"

# Check if data exists in ScyllaDB
docker exec scylla cqlsh -e \
  "SELECT COUNT(*) FROM app_data.users;"

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep cdc

# Check topic messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic cdc.scylla.users \
  --from-beginning \
  --max-messages 10
```

**Common Causes and Solutions**:

#### Cause 1: CDC Not Enabled

**Solution**:
```bash
# Enable CDC on tables
docker exec scylla cqlsh -e \
  "ALTER TABLE app_data.users WITH cdc = {'enabled': 'true', 'preimage': 'true'};"

# Restart source connector
./scripts/deploy-connector.sh restart scylla-source
```

#### Cause 2: No Data Changes

**Solution**:
```bash
# Generate test data
./scripts/test-replication.sh

# Or insert manually
docker exec scylla cqlsh -e \
  "INSERT INTO app_data.users (user_id, username, email, created_at, updated_at, status)
   VALUES (uuid(), 'testuser', 'test@example.com', toTimestamp(now()), toTimestamp(now()), 'active');"

# Wait for replication
sleep 10

# Verify in PostgreSQL
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT COUNT(*) FROM cdc_data.users WHERE username = 'testuser';"
```

#### Cause 3: Topic Routing Misconfigured

**Solution**:
```bash
# Check transform configuration
curl -s http://localhost:8083/connectors/scylla-source/config | jq '.["transforms.route.regex"]'

# Expected: "cdc\\.scylla\\.app_data\\.(.*)"

# If incorrect, fix and update
vim configs/connectors/scylla-source.json
./scripts/deploy-connector.sh update scylla-source
```

### Issue 6: Duplicate Records

**Symptoms**:
- Primary key violations in PostgreSQL
- Multiple records with same key
- Error: "duplicate key value violates unique constraint"

**Diagnosis**:

```bash
# Check for duplicates in PostgreSQL
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT user_id, COUNT(*)
   FROM cdc_data.users
   GROUP BY user_id
   HAVING COUNT(*) > 1;"

# Check if idempotence is enabled
curl -s http://localhost:8083/connectors/scylla-source/config | \
  jq '.["producer.override.enable.idempotence"]'
```

**Solution**:

```bash
# Enable idempotence in source connector
vim configs/connectors/scylla-source.json

# Ensure these settings:
"producer.override.enable.idempotence": true
"producer.override.acks": "all"
"producer.override.max.in.flight.requests.per.connection": 1

# Update connector
./scripts/deploy-connector.sh update scylla-source

# Configure sink for upsert mode
vim configs/connectors/postgres-sink.json

# Set:
"insert.mode": "upsert"
"pk.mode": "record_key"

# Update sink connector
./scripts/deploy-connector.sh update postgres-sink

# Clean up duplicates
docker exec postgres psql -U postgres -d warehouse -c \
  "DELETE FROM cdc_data.users a USING cdc_data.users b
   WHERE a.ctid < b.ctid AND a.user_id = b.user_id;"
```

---

## Schema Evolution Issues

### Issue 7: Schema Compatibility Failure

**Symptoms**:
- Error: `org.apache.kafka.connect.errors.SchemaBuilderException`
- Messages in DLQ with schema errors
- Alert: "Schema Compatibility Failure"

**Diagnosis**:

```bash
# Check schema versions
curl -s http://localhost:8081/subjects | jq .

# Get latest schema
curl -s http://localhost:8081/subjects/cdc.scylla.users-value/versions/latest | jq .

# Check compatibility mode
curl -s http://localhost:8081/config/cdc.scylla.users-value | jq .

# Check DLQ for schema errors
./scripts/check-dlq.sh | grep -i schema
```

**Solution**:

```bash
# Option 1: Change compatibility mode (less restrictive)
curl -X PUT http://localhost:8081/config/cdc.scylla.users-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FORWARD"}'

# Option 2: Register compatible schema manually
# Create compatible schema file
cat > /tmp/users-schema.avsc << 'EOF'
{
  "type": "record",
  "name": "users",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "username", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "new_field", "type": ["null", "string"], "default": null}
  ]
}
EOF

# Register schema
curl -X POST http://localhost:8081/subjects/cdc.scylla.users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schema\": $(cat /tmp/users-schema.avsc | jq -c . | jq -R .)}"

# Restart connector
./scripts/deploy-connector.sh restart scylla-source

# Replay DLQ messages
./scripts/replay-dlq.sh --topic dlq-scylla-source
```

### Issue 8: Missing Columns After Schema Change

**Symptoms**:
- New columns not appearing in PostgreSQL
- NULL values for new columns
- Warning in connector logs about unknown fields

**Diagnosis**:

```bash
# Compare schemas
docker exec scylla cqlsh -e "DESCRIBE TABLE app_data.users;"
docker exec postgres psql -U postgres -d warehouse -c "\\d cdc_data.users"

# Check if auto-create is enabled
curl -s http://localhost:8083/connectors/postgres-sink/config | \
  jq '.["auto.create"], .["auto.evolve"]'
```

**Solution**:

```bash
# Enable auto schema evolution
vim configs/connectors/postgres-sink.json

# Set:
"auto.create": "true"
"auto.evolve": "true"

# Update sink connector
./scripts/deploy-connector.sh update postgres-sink

# Or manually add column to PostgreSQL
docker exec postgres psql -U postgres -d warehouse -c \
  "ALTER TABLE cdc_data.users ADD COLUMN IF NOT EXISTS new_field VARCHAR(255);"

# Restart sink connector
./scripts/deploy-connector.sh restart postgres-sink
```

---

## Performance Issues

### Issue 9: High CPU Usage

**Symptoms**:
- CPU usage >80% on Kafka Connect worker
- Slow message processing
- High GC (Garbage Collection) activity

**Diagnosis**:

```bash
# Check CPU usage
docker stats kafka-connect

# Check GC logs
docker-compose logs kafka-connect | grep -i "gc pause"

# Check thread count
docker exec kafka-connect jps -l
docker exec kafka-connect jstack <pid> | grep "nid=" | wc -l
```

**Solution**:

```bash
# Increase heap size
vim docker-compose.yml

# Change:
KAFKA_HEAP_OPTS: "-Xms4G -Xmx8G"

# Tune GC settings
KAFKA_JVM_PERFORMANCE_OPTS: >-
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=20
  -XX:InitiatingHeapOccupancyPercent=35
  -XX:G1HeapRegionSize=16M

# Restart Kafka Connect
docker-compose restart kafka-connect
sleep 60

# Scale horizontally if still high
docker-compose up -d --scale kafka-connect=6
```

### Issue 10: High Memory Usage

**Symptoms**:
- Memory usage >90%
- OutOfMemoryError in logs
- Connector tasks failing

**Diagnosis**:

```bash
# Check memory usage
docker stats kafka-connect

# Check heap dump
docker exec kafka-connect jmap -heap <pid>
```

**Solution**:

```bash
# Reduce batch size
vim configs/connectors/postgres-sink.json

# Change:
"batch.size": "5000"
# To:
"batch.size": "1000"

# Reduce buffer size
vim configs/connectors/scylla-source.json

# Change:
"max.queue.size": "8192"
# To:
"max.queue.size": "4096"

# Update connectors
./scripts/deploy-connector.sh update scylla-source
./scripts/deploy-connector.sh update postgres-sink

# Increase heap if needed
vim docker-compose.yml
# KAFKA_HEAP_OPTS: "-Xms8G -Xmx16G"

docker-compose restart kafka-connect
```

### Issue 11: Slow PostgreSQL Writes

**Symptoms**:
- Sink connector throughput <1000 records/sec
- High database CPU usage
- Long-running transactions

**Diagnosis**:

```bash
# Check PostgreSQL performance
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT schemaname, tablename, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch
   FROM pg_stat_user_tables;"

# Check missing indexes
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT schemaname, tablename, attname, n_distinct, correlation
   FROM pg_stats
   WHERE schemaname = 'cdc_data' AND n_distinct > 100;"

# Check table bloat
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT schemaname, tablename,
          pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
   FROM pg_tables
   WHERE schemaname = 'cdc_data'
   ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
```

**Solution**:

```bash
# Add missing indexes
docker exec postgres psql -U postgres -d warehouse << 'EOF'
CREATE INDEX IF NOT EXISTS idx_users_email ON cdc_data.users(email);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON cdc_data.users(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON cdc_data.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON cdc_data.orders(order_date);
EOF

# Run VACUUM ANALYZE
docker exec postgres psql -U postgres -d warehouse -c \
  "VACUUM ANALYZE cdc_data.users;"

# Increase connection pool
vim configs/connectors/postgres-sink.json

# Change:
"connection.pool.size": "10"
# To:
"connection.pool.size": "20"

# Update sink connector
./scripts/deploy-connector.sh update postgres-sink

# Tune PostgreSQL
vim configs/postgres/postgresql.conf

# Optimize for writes:
checkpoint_completion_target = 0.9
wal_buffers = 16MB
maintenance_work_mem = 2GB
max_wal_size = 4GB

docker-compose restart postgres
```

---

## Data Consistency Issues

### Issue 12: Data Discrepancy Detected

**Symptoms**:
- Alert: "High Data Discrepancy"
- `cdc_data_accuracy_percentage < 95%`
- Reconciliation reports missing/mismatched rows

**Diagnosis**:

```bash
# Run full reconciliation
./scripts/reconcile.py full --table users --dry-run

# Check discrepancy report
cat /tmp/reconciliation-report-*.json | jq .

# Identify discrepancy types
cat /tmp/reconciliation-report-*.json | \
  jq '.discrepancies | {missing: .missing_in_target | length, extra: .extra_in_target | length, mismatched: .mismatches | length}'
```

**Solution**:

```bash
# Execute repairs
./scripts/reconcile.py full --table users --execute-repairs

# For all tables
./scripts/reconcile.py full --all-tables --execute-repairs

# Monitor accuracy recovery
watch -n 60 'curl -s http://localhost:9090/api/v1/query?query=cdc_data_accuracy_percentage'

# If discrepancies persist, investigate root cause
# Check connector errors
docker-compose logs kafka-connect | grep ERROR

# Check DLQ
./scripts/check-dlq.sh

# Check for constraint violations
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT conname, contype, pg_get_constraintdef(oid)
   FROM pg_constraint
   WHERE connamespace = 'cdc_data'::regnamespace;"
```

### Issue 13: Missing Rows

**Symptoms**:
- Row count mismatch between ScyllaDB and PostgreSQL
- Reconciliation reports many missing_in_target

**Diagnosis**:

```bash
# Compare row counts
docker exec scylla cqlsh -e "SELECT COUNT(*) FROM app_data.users;"
docker exec postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM cdc_data.users;"

# Check if messages reached Kafka
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic cdc.scylla.users

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-sink
```

**Solution**:

```bash
# Check DLQ for failed inserts
./scripts/check-dlq.sh

# Replay DLQ messages
./scripts/replay-dlq.sh --topic dlq-postgres-sink

# Run full reconciliation with repairs
./scripts/reconcile.py full --table users --execute-repairs

# If issue persists, reset connector offsets and re-snapshot
./scripts/deploy-connector.sh delete scylla-source

# Configure for full snapshot
vim configs/connectors/scylla-source.json
# Set: "snapshot.mode": "initial"

./scripts/deploy-connector.sh create scylla-source
```

---

## Infrastructure Issues

### Issue 14: Kafka Disk Space Full

**Symptoms**:
- Error: `No space left on device`
- Kafka broker not accepting new messages
- Log compaction failing

**Diagnosis**:

```bash
# Check disk usage
docker exec kafka df -h

# Check topic sizes
docker exec kafka du -sh /var/lib/kafka/data/*
```

**Solution**:

```bash
# Delete old topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic <old-topic>

# Reduce retention period
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name cdc.scylla.users \
  --alter --add-config retention.ms=43200000  # 12 hours

# Clean up log segments
docker exec kafka kafka-run-class kafka.tools.DumpLogSegments \
  --deep-iteration --print-data-log \
  --files /var/lib/kafka/data/cdc.scylla.users-0/*.log

# Add more disk space (production)
# Mount additional volume in docker-compose.yml
```

### Issue 15: Network Connectivity Issues

**Symptoms**:
- Connection timeouts
- `Connection refused` errors
- Intermittent failures

**Diagnosis**:

```bash
# Check network connectivity
docker network ls
docker network inspect scylla-pg-cdc_default

# Test connectivity between services
docker exec kafka-connect nc -zv scylla 9042
docker exec kafka-connect nc -zv postgres 5432
docker exec kafka-connect nc -zv kafka 9092

# Check DNS resolution
docker exec kafka-connect nslookup scylla
docker exec kafka-connect nslookup postgres
```

**Solution**:

```bash
# Restart networking
docker-compose down
docker network prune -f
docker-compose up -d

# Or recreate specific service
docker-compose up -d --force-recreate kafka-connect

# Check firewall rules (if applicable)
sudo iptables -L -n | grep <port>
```

---

## Monitoring and Alerting Issues

### Issue 16: Metrics Not Showing in Grafana

**Symptoms**:
- Empty dashboards
- "No data" in panels
- Metrics queries returning no results

**Diagnosis**:

```bash
# Check Prometheus is scraping metrics
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health: .health}'

# Check if metrics exist
curl -s 'http://localhost:9090/api/v1/query?query=cdc_connector_healthy' | jq .

# Check Grafana datasource
curl -s http://admin:admin@localhost:3000/api/datasources | jq .
```

**Solution**:

```bash
# Restart Prometheus
docker-compose restart prometheus
sleep 30

# Check metrics are being exported
curl -s http://localhost:9090/metrics | grep cdc_

# Verify Grafana datasource configuration
# Login to Grafana: http://localhost:3000
# Configuration → Data Sources → Prometheus
# Test connection

# Reimport dashboards if needed
for dashboard in docker/grafana/dashboards/*.json; do
  curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
    -H "Content-Type: application/json" \
    -d @"$dashboard"
done
```

### Issue 17: Alerts Not Firing

**Symptoms**:
- Known issues not triggering alerts
- AlertManager shows no alerts
- No notifications received

**Diagnosis**:

```bash
# Check alert rules are loaded
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[] | .name'

# Check if alert conditions are met
curl -s 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds > 300' | jq .

# Check AlertManager is running
curl -s http://localhost:9093/api/v2/status | jq .

# Check firing alerts
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[] | {name: .labels.alertname, state: .state}'
```

**Solution**:

```bash
# Reload alert rules
curl -X POST http://localhost:9090/-/reload

# Or restart Prometheus
docker-compose restart prometheus

# Verify alert rule syntax
docker exec prometheus promtool check rules /etc/prometheus/alert-rules.yml

# Test alert condition manually
curl -s 'http://localhost:9090/api/v1/query?query=cdc_connector_healthy == 0' | jq .
```

---

## Appendix

### Useful Diagnostic Commands

```bash
# Service health
./scripts/health-check.sh
docker-compose ps
docker-compose logs --tail=100 <service>

# Connector status
curl -s http://localhost:8083/connectors | jq .
curl -s http://localhost:8083/connectors/<name>/status | jq .

# Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic <topic> --from-beginning --max-messages 10

# Database queries
docker exec scylla cqlsh -e "SELECT COUNT(*) FROM app_data.users;"
docker exec postgres psql -U postgres -d warehouse -c "SELECT COUNT(*) FROM cdc_data.users;"

# Metrics
curl -s 'http://localhost:9090/api/v1/query?query=<metric>' | jq .

# DLQ
./scripts/check-dlq.sh
./scripts/replay-dlq.sh --topic <dlq-topic>

# Reconciliation
./scripts/reconcile.py full --table <table> --dry-run
./scripts/reconcile.py status
```

### Log Locations

```bash
# All service logs
docker-compose logs -f

# Specific service
docker-compose logs -f kafka-connect

# Recent errors
docker-compose logs --since=1h | grep -i error

# Connector logs
docker-compose logs kafka-connect | grep <connector-name>

# With timestamps
docker-compose logs -f -t
```

### Emergency Recovery

**Complete Pipeline Failure**:
```bash
# 1. Stop all services
docker-compose down

# 2. Backup data
./scripts/backup-offsets.sh
docker exec postgres pg_dump -U postgres -d warehouse -F c -f /backups/emergency-backup.dump

# 3. Restart infrastructure
docker-compose up -d scylla postgres kafka zookeeper schema-registry

# 4. Verify core services
./scripts/health-check.sh

# 5. Restart Kafka Connect
docker-compose up -d kafka-connect
sleep 60

# 6. Redeploy connectors
./scripts/deploy-connector.sh create scylla-source
./scripts/deploy-connector.sh create postgres-sink

# 7. Run reconciliation
./scripts/reconcile.py full --all-tables --execute-repairs
```

**Data Loss Recovery**:
```bash
# 1. Stop sink connector
./scripts/deploy-connector.sh delete postgres-sink

# 2. Restore PostgreSQL from backup
docker exec postgres pg_restore -U postgres -d warehouse -c /backups/warehouse-latest.dump

# 3. Restore offsets
./scripts/restore-offsets.sh /backups/offsets/offsets-latest.json

# 4. Redeploy sink connector
./scripts/deploy-connector.sh create postgres-sink

# 5. Verify data
./scripts/reconcile.py full --all-tables --dry-run
```

### Getting Help

1. **Check documentation**: [docs/](.)
2. **Search logs**: `docker-compose logs | grep -i <error>`
3. **Check monitoring**: Grafana dashboards, Prometheus alerts
4. **Run diagnostics**: `./scripts/health-check.sh`
5. **Contact support**: [Slack/Email/Phone]

### Related Documentation

- [Architecture](architecture.md) - System architecture
- [Runbook](runbook.md) - Operational procedures
- [Scaling Guide](scaling.md) - Capacity planning
- [README](../README.md) - Project overview

---

**Document Version**: 1.0
**Last Updated**: 2025-12-09
**Maintained By**: Platform Team
**Review Cycle**: Monthly
