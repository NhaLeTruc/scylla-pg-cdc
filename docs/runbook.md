# ScyllaDB to PostgreSQL CDC Pipeline - Operations Runbook

## Overview

This runbook provides operational procedures for deploying, managing, and troubleshooting the CDC pipeline in production environments.

## Table of Contents

1. [Deployment Procedures](#deployment-procedures)
2. [Health Checks](#health-checks)
3. [Scaling Operations](#scaling-operations)
4. [Backup and Recovery](#backup-and-recovery)
5. [Troubleshooting](#troubleshooting)
6. [Maintenance Windows](#maintenance-windows)
7. [Emergency Procedures](#emergency-procedures)
8. [Monitoring and Alerting](#monitoring-and-alerting)

---

## Deployment Procedures

### Initial Deployment

#### Prerequisites

1. Verify infrastructure is provisioned:
   ```bash
   # Check Docker is installed and running
   docker --version
   docker-compose --version

   # Check resource availability
   free -h  # At least 16 GB RAM
   df -h    # At least 100 GB disk space
   nproc    # At least 8 CPU cores
   ```

2. Clone repository:
   ```bash
   git clone <repository-url>
   cd scylla-pg-cdc
   ```

3. Configure environment:
   ```bash
   # Copy environment template
   cp .env.example .env

   # Edit configuration
   vim .env
   ```

#### Step-by-Step Deployment

**Step 1: Start Core Infrastructure (5 minutes)**

```bash
# Start ScyllaDB, PostgreSQL, Kafka, Zookeeper
docker-compose up -d scylla postgres kafka zookeeper

# Wait for services to be ready
sleep 60

# Verify services are healthy
./scripts/health-check.sh
```

Expected output:
```
[✓] ScyllaDB is healthy
[✓] PostgreSQL is healthy
[✓] Kafka is healthy
[✓] Zookeeper is healthy
```

**Step 2: Initialize Schemas (2 minutes)**

```bash
# Create ScyllaDB keyspace and tables
./scripts/setup-scylla.sh

# Create PostgreSQL schema and tables
./scripts/setup-postgres.sh

# Verify schemas
./scripts/verify-schemas.sh
```

**Step 3: Start Kafka Ecosystem (3 minutes)**

```bash
# Start Schema Registry
docker-compose up -d schema-registry

# Wait for Schema Registry
sleep 30

# Start Kafka Connect
docker-compose up -d kafka-connect

# Wait for Kafka Connect
sleep 30

# Verify Kafka Connect is ready
curl -s http://localhost:8083/ | jq .
```

**Step 4: Deploy Connectors (2 minutes)**

```bash
# Deploy ScyllaDB source connector
./scripts/deploy-connector.sh create scylla-source

# Wait for connector to start
sleep 10

# Deploy PostgreSQL sink connector
./scripts/deploy-connector.sh create postgres-sink

# Verify connectors are running
./scripts/health-check.sh
```

Expected output:
```
[✓] Connector 'scylla-source' is RUNNING (4/4 tasks)
[✓] Connector 'postgres-sink' is RUNNING (4/4 tasks)
```

**Step 5: Start Observability Stack (2 minutes)**

```bash
# Start Prometheus, Grafana, Jaeger
docker-compose up -d prometheus grafana jaeger

# Wait for services
sleep 30

# Access dashboards
echo "Grafana: http://localhost:3000 (admin/admin)"
echo "Prometheus: http://localhost:9090"
echo "Jaeger: http://localhost:16686"
```

**Step 6: Validate Deployment (5 minutes)**

```bash
# Run end-to-end test
./scripts/test-replication.sh

# Check metrics
curl -s http://localhost:9090/api/v1/query?query=cdc_connector_healthy | jq .

# View Grafana dashboards
open http://localhost:3000/d/cdc-connector-health
```

**Total Deployment Time**: ~20 minutes

### Connector Deployment

#### Deploy New Connector

```bash
# Create connector from config file
./scripts/deploy-connector.sh create <connector-name>

# Example: Deploy custom connector
./scripts/deploy-connector.sh create custom-source configs/connectors/custom-source.json
```

#### Update Existing Connector

```bash
# Update connector configuration
./scripts/deploy-connector.sh update <connector-name>

# Verify update
curl -s http://localhost:8083/connectors/<connector-name>/status | jq .
```

#### Delete Connector

```bash
# Delete connector (preserves offsets)
./scripts/deploy-connector.sh delete <connector-name>

# Confirm deletion
./scripts/deploy-connector.sh list
```

### Rolling Updates

#### Update Connector Configuration

```bash
# 1. Update configuration file
vim configs/connectors/scylla-source.json

# 2. Apply updated configuration
curl -X PUT http://localhost:8083/connectors/scylla-source/config \
  -H "Content-Type: application/json" \
  -d @configs/connectors/scylla-source.json

# 3. Verify connector restarts with new config
./scripts/health-check.sh
```

#### Update Kafka Connect Worker

```bash
# 1. Backup offsets
./scripts/backup-offsets.sh

# 2. Stop one worker
docker-compose stop kafka-connect-1

# 3. Update worker configuration
vim configs/kafka-connect/worker.properties

# 4. Restart worker
docker-compose up -d kafka-connect-1

# 5. Wait for rebalance
sleep 60

# 6. Verify tasks redistributed
curl -s http://localhost:8083/connectors/scylla-source/tasks | jq .

# 7. Repeat for remaining workers
```

---

## Health Checks

### Automated Health Check

```bash
# Run comprehensive health check
./scripts/health-check.sh

# Check specific component
./scripts/health-check.sh --component scylla
./scripts/health-check.sh --component postgres
./scripts/health-check.sh --component kafka
./scripts/health-check.sh --component connectors
```

### Manual Health Checks

#### ScyllaDB Health

```bash
# Check node status
docker exec scylla nodetool status

# Check CDC logs
docker exec scylla cqlsh -e "SELECT COUNT(*) FROM system_distributed.cdc_streams_descriptions_v2;"

# Check connection
docker exec scylla cqlsh -e "SELECT now() FROM system.local;"
```

Expected: Connection succeeds, CDC streams exist

#### PostgreSQL Health

```bash
# Check database is accepting connections
docker exec postgres pg_isready -U postgres -d warehouse

# Check replication lag (if using replicas)
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT now() - pg_last_xact_replay_timestamp() AS replication_lag;"

# Check table row counts
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT schemaname, tablename, n_live_tup FROM pg_stat_user_tables;"
```

#### Kafka Health

```bash
# Check broker is running
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-sink
```

#### Kafka Connect Health

```bash
# Check worker is running
curl -s http://localhost:8083/ | jq .

# List connectors
curl -s http://localhost:8083/connectors | jq .

# Check connector status
curl -s http://localhost:8083/connectors/scylla-source/status | jq .

# Check task status
curl -s http://localhost:8083/connectors/scylla-source/tasks/0/status | jq .
```

Expected: All connectors RUNNING, all tasks RUNNING

#### Schema Registry Health

```bash
# Check Schema Registry is running
curl -s http://localhost:8081/ | jq .

# List subjects (schemas)
curl -s http://localhost:8081/subjects | jq .

# Get schema versions
curl -s http://localhost:8081/subjects/cdc.scylla.users-value/versions | jq .
```

### Metrics-Based Health Checks

```bash
# Check replication lag
curl -s 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | \
  jq '.data.result[0].value[1]'

# Check error rate
curl -s 'http://localhost:9090/api/v1/query?query=rate(cdc_replication_errors_total[5m])' | \
  jq '.data.result[0].value[1]'

# Check connector health
curl -s 'http://localhost:9090/api/v1/query?query=cdc_connector_healthy' | \
  jq '.data.result[].value[1]'
```

Expected: Lag <60s, Error rate = 0, Connector healthy = 1

---

## Scaling Operations

### Horizontal Scaling

#### Scale Kafka Connect Workers

```bash
# Add new worker to cluster
docker-compose up -d --scale kafka-connect=5

# Wait for worker to join
sleep 30

# Verify rebalance
curl -s http://localhost:8083/connectors/scylla-source/tasks | jq .

# Expected: Tasks distributed across 5 workers
```

#### Scale Kafka Brokers

```bash
# Add new broker
docker-compose up -d kafka-broker-4

# Wait for broker to join
sleep 60

# Reassign partitions
docker exec kafka kafka-reassign-partitions \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassignment.json \
  --execute

# Monitor reassignment progress
docker exec kafka kafka-reassign-partitions \
  --bootstrap-server localhost:9092 \
  --reassignment-json-file /tmp/reassignment.json \
  --verify
```

#### Scale ScyllaDB Cluster

```bash
# Add new ScyllaDB node
docker-compose up -d scylla-node-4

# Wait for node to join
docker exec scylla nodetool status

# Run repair on new node
docker exec scylla-node-4 nodetool repair

# Monitor repair progress
docker exec scylla-node-4 nodetool compactionstats
```

### Vertical Scaling

#### Increase Kafka Connect Memory

```bash
# 1. Update docker-compose.yml
vim docker-compose.yml

# Change:
# KAFKA_HEAP_OPTS: "-Xms2G -Xmx4G"
# To:
# KAFKA_HEAP_OPTS: "-Xms4G -Xmx8G"

# 2. Restart Kafka Connect (rolling restart)
docker-compose restart kafka-connect

# 3. Verify new memory allocation
docker stats kafka-connect
```

#### Increase PostgreSQL Resources

```bash
# 1. Update PostgreSQL configuration
vim configs/postgres/postgresql.conf

# Increase:
shared_buffers = 8GB          # from 4GB
effective_cache_size = 24GB   # from 12GB
maintenance_work_mem = 2GB    # from 1GB

# 2. Restart PostgreSQL (requires downtime)
docker-compose restart postgres

# 3. Verify configuration
docker exec postgres psql -U postgres -c "SHOW shared_buffers;"
```

---

## Backup and Recovery

### Backup Procedures

#### Backup Kafka Connect Offsets

```bash
# Backup offsets for disaster recovery
./scripts/backup-offsets.sh

# Output: /backups/offsets/offsets-<timestamp>.json

# Verify backup
cat /backups/offsets/offsets-latest.json | jq .
```

#### Backup PostgreSQL Data

```bash
# Full database backup
docker exec postgres pg_dump -U postgres -d warehouse -F c \
  -f /backups/warehouse-$(date +%Y%m%d).dump

# Backup specific schema
docker exec postgres pg_dump -U postgres -d warehouse -n cdc_data -F c \
  -f /backups/cdc_data-$(date +%Y%m%d).dump

# Verify backup
docker exec postgres pg_restore --list /backups/warehouse-$(date +%Y%m%d).dump
```

#### Backup ScyllaDB Data

```bash
# Create snapshot
docker exec scylla nodetool snapshot -t backup-$(date +%Y%m%d)

# List snapshots
docker exec scylla nodetool listsnapshots

# Copy snapshot files to backup location
docker exec scylla tar czf /backups/scylla-backup-$(date +%Y%m%d).tar.gz \
  /var/lib/scylla/data/app_data/*/snapshots/
```

### Recovery Procedures

#### Restore Kafka Connect Offsets

```bash
# 1. Stop connectors
./scripts/deploy-connector.sh delete scylla-source
./scripts/deploy-connector.sh delete postgres-sink

# 2. Restore offsets from backup
./scripts/restore-offsets.sh /backups/offsets/offsets-20250101.json

# 3. Redeploy connectors
./scripts/deploy-connector.sh create scylla-source
./scripts/deploy-connector.sh create postgres-sink

# 4. Verify connectors resume from restored offsets
curl -s http://localhost:8083/connectors/scylla-source/status | jq .
```

#### Restore PostgreSQL Data

```bash
# 1. Stop sink connector
./scripts/deploy-connector.sh delete postgres-sink

# 2. Drop and recreate database
docker exec postgres psql -U postgres -c "DROP DATABASE warehouse;"
docker exec postgres psql -U postgres -c "CREATE DATABASE warehouse;"

# 3. Restore from backup
docker exec postgres pg_restore -U postgres -d warehouse \
  /backups/warehouse-20250101.dump

# 4. Verify restoration
docker exec postgres psql -U postgres -d warehouse -c \
  "SELECT COUNT(*) FROM cdc_data.users;"

# 5. Restart sink connector
./scripts/deploy-connector.sh create postgres-sink
```

#### Disaster Recovery

**Scenario**: Complete data center failure

**Steps**:

1. **Restore infrastructure** in new data center
   ```bash
   # Deploy all services
   docker-compose up -d
   ```

2. **Restore ScyllaDB data**
   ```bash
   # Copy snapshot files to new nodes
   # Restore snapshot
   docker exec scylla nodetool refresh app_data users
   ```

3. **Restore PostgreSQL data**
   ```bash
   docker exec postgres pg_restore -U postgres -d warehouse \
     /backups/warehouse-latest.dump
   ```

4. **Restore Kafka Connect offsets**
   ```bash
   ./scripts/restore-offsets.sh /backups/offsets/offsets-latest.json
   ```

5. **Deploy connectors**
   ```bash
   ./scripts/deploy-connector.sh create scylla-source
   ./scripts/deploy-connector.sh create postgres-sink
   ```

6. **Run reconciliation** to validate data consistency
   ```bash
   ./scripts/reconcile.py full --all-tables --execute-repairs
   ```

**RTO (Recovery Time Objective)**: 4 hours
**RPO (Recovery Point Objective)**: 24 hours (daily backups)

---

## Troubleshooting

### Quick Diagnostics

```bash
# Run health check
./scripts/health-check.sh

# Check recent errors in logs
docker-compose logs --tail=100 kafka-connect | grep ERROR

# Check DLQ for failed messages
./scripts/check-dlq.sh

# Check replication lag
curl -s 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq .
```

### Common Issues

#### Issue: Connector in FAILED State

**Symptoms**: `curl http://localhost:8083/connectors/scylla-source/status` shows FAILED

**Diagnosis**:
```bash
# Check connector status and error
curl -s http://localhost:8083/connectors/scylla-source/status | jq '.tasks[].trace'

# Check logs
docker-compose logs kafka-connect | grep scylla-source
```

**Resolution**:
```bash
# Restart connector
curl -X POST http://localhost:8083/connectors/scylla-source/restart

# If still failing, recreate
./scripts/deploy-connector.sh delete scylla-source
./scripts/deploy-connector.sh create scylla-source
```

#### Issue: High Replication Lag

**Symptoms**: `cdc_replication_lag_seconds > 300`

**Diagnosis**:
```bash
# Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-sink

# Check sink connector throughput
curl -s 'http://localhost:9090/api/v1/query?query=rate(cdc_records_replicated_total[5m])' | jq .
```

**Resolution**:
```bash
# Increase parallelism
vim configs/connectors/postgres-sink.json
# Set "tasks.max": "8"

# Update connector
./scripts/deploy-connector.sh update postgres-sink

# Monitor lag decrease
watch -n 5 'curl -s http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds'
```

#### Issue: Schema Compatibility Failure

**Symptoms**: `SchemaCompatibilityException` in logs

**Diagnosis**:
```bash
# Check schema versions
curl -s http://localhost:8081/subjects/cdc.scylla.users-value/versions | jq .

# Get latest schema
curl -s http://localhost:8081/subjects/cdc.scylla.users-value/versions/latest | jq .
```

**Resolution**:
```bash
# Register compatible schema manually
curl -X POST http://localhost:8081/subjects/cdc.scylla.users-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @schemas/users-v2.avsc

# Or update compatibility mode
curl -X PUT http://localhost:8081/config/cdc.scylla.users-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FORWARD"}'
```

For more troubleshooting scenarios, see [docs/troubleshooting.md](troubleshooting.md).

---

## Maintenance Windows

### Planned Maintenance Checklist

**Pre-Maintenance (T-24h)**:
- [ ] Announce maintenance window to stakeholders
- [ ] Backup all offsets: `./scripts/backup-offsets.sh`
- [ ] Backup PostgreSQL: `pg_dump`
- [ ] Backup ScyllaDB: `nodetool snapshot`
- [ ] Document current state (versions, configurations)
- [ ] Prepare rollback plan

**During Maintenance**:
- [ ] Stop connectors: `./scripts/deploy-connector.sh delete <name>`
- [ ] Perform maintenance tasks (upgrades, configuration changes)
- [ ] Restart services in order: ScyllaDB → Kafka → Kafka Connect
- [ ] Redeploy connectors: `./scripts/deploy-connector.sh create <name>`
- [ ] Run health checks: `./scripts/health-check.sh`
- [ ] Verify replication: `./scripts/test-replication.sh`

**Post-Maintenance**:
- [ ] Monitor metrics for 1 hour
- [ ] Run reconciliation: `./scripts/reconcile.py full --all-tables`
- [ ] Validate data accuracy: Check `cdc_data_accuracy_percentage`
- [ ] Document changes and issues
- [ ] Announce maintenance completion

### Zero-Downtime Deployment

For rolling updates without downtime:

1. **Update connectors one at a time**:
   ```bash
   # Update source connector (minimal disruption)
   ./scripts/deploy-connector.sh update scylla-source

   # Wait for tasks to stabilize
   sleep 60

   # Update sink connector
   ./scripts/deploy-connector.sh update postgres-sink
   ```

2. **Rolling worker updates**:
   ```bash
   for worker in kafka-connect-{1..3}; do
     docker-compose stop $worker
     # Update configuration
     docker-compose up -d $worker
     # Wait for rebalance
     sleep 120
   done
   ```

---

## Emergency Procedures

### Emergency Contacts

- **Oncall Engineer**: [Pager Duty]
- **Database Team**: [Slack #db-oncall]
- **Platform Team**: [Slack #platform-oncall]
- **Manager**: [Phone/Email]

### Severity Levels

**P0 - Critical**: Data loss, complete service outage
- Response time: 15 minutes
- Resolution target: 2 hours

**P1 - High**: Partial outage, high error rate
- Response time: 1 hour
- Resolution target: 4 hours

**P2 - Medium**: Degraded performance, non-critical alerts
- Response time: 4 hours
- Resolution target: 24 hours

### Emergency Response Procedures

#### P0: Complete Pipeline Failure

**Symptoms**: All connectors down, no data replication

**Immediate Actions**:
```bash
# 1. Check infrastructure
./scripts/health-check.sh

# 2. Review recent changes
git log --oneline -10

# 3. Check for alerts
curl -s http://localhost:9090/api/v1/alerts | jq .

# 4. Attempt quick restart
docker-compose restart kafka-connect

# 5. If restart fails, rollback to last known good config
git checkout HEAD~1 configs/
./scripts/deploy-connector.sh update scylla-source
./scripts/deploy-connector.sh update postgres-sink
```

**Escalation**: If not resolved in 30 minutes, escalate to platform team

#### P1: Data Discrepancy >10%

**Symptoms**: `cdc_data_accuracy_percentage < 90`

**Immediate Actions**:
```bash
# 1. Stop sink connector to prevent further discrepancies
./scripts/deploy-connector.sh delete postgres-sink

# 2. Investigate discrepancy
./scripts/reconcile.py full --all-tables --dry-run

# 3. Analyze discrepancy report
cat /tmp/reconciliation-report-*.json | jq .

# 4. If root cause identified, fix and redeploy
./scripts/deploy-connector.sh create postgres-sink

# 5. Run full reconciliation with repairs
./scripts/reconcile.py full --all-tables --execute-repairs

# 6. Monitor accuracy recovery
watch -n 60 'curl -s http://localhost:9090/api/v1/query?query=cdc_data_accuracy_percentage'
```

#### P1: DLQ Accumulation >1000 Messages

**Symptoms**: `cdc_dlq_messages_total > 1000`

**Immediate Actions**:
```bash
# 1. Check DLQ for error patterns
./scripts/check-dlq.sh

# 2. Identify root cause
docker-compose logs kafka-connect | grep ERROR | tail -50

# 3. Fix root cause (schema, configuration, data)
# Example: Fix schema compatibility
curl -X PUT http://localhost:8081/config/cdc.scylla.users-value \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility": "FORWARD"}'

# 4. Replay DLQ messages
./scripts/replay-dlq.sh --topic dlq-scylla-source

# 5. Monitor DLQ count decrease
watch -n 30 './scripts/check-dlq.sh'
```

---

## Monitoring and Alerting

### Dashboard Access

- **Grafana**: http://localhost:3000 (admin/admin)
  - CDC Connector Health
  - Schema Evolution
  - Reconciliation

- **Prometheus**: http://localhost:9090
  - Metrics queries
  - Alert status

- **Jaeger**: http://localhost:16686
  - Distributed traces
  - Performance analysis

### Critical Alerts

| Alert | Threshold | Action |
|-------|-----------|--------|
| Connector Down | >2 minutes | Restart connector, check logs |
| High Replication Lag | >300s | Increase parallelism, check sink |
| Critical Replication Lag | >900s | P1 incident, investigate immediately |
| High Error Rate | >0.1 errors/sec | Check DLQ, review connector logs |
| Data Accuracy Low | <95% | Run reconciliation, investigate |
| Critical Data Accuracy | <90% | P1 incident, stop sink, investigate |
| DLQ Backlog | >100 messages | Investigate error patterns |
| Critical DLQ Backlog | >1000 messages | P1 incident, fix root cause |

### Log Locations

```bash
# Kafka Connect logs
docker-compose logs -f kafka-connect

# ScyllaDB logs
docker-compose logs -f scylla

# PostgreSQL logs
docker-compose logs -f postgres

# All service logs
docker-compose logs -f
```

### Metrics Queries

```bash
# Replication lag
curl -s 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq .

# Throughput (records/sec)
curl -s 'http://localhost:9090/api/v1/query?query=rate(cdc_records_replicated_total[5m])' | jq .

# Error rate
curl -s 'http://localhost:9090/api/v1/query?query=rate(cdc_replication_errors_total[5m])' | jq .

# Data accuracy
curl -s 'http://localhost:9090/api/v1/query?query=cdc_data_accuracy_percentage' | jq .
```

---

## Best Practices

### Deployment

1. **Always backup before changes**: Offsets, configurations, data
2. **Use rolling updates**: Minimize downtime
3. **Test in staging first**: Validate changes before production
4. **Document changes**: Update runbook and changelog
5. **Monitor after deployment**: Watch metrics for 1 hour

### Operations

1. **Run daily reconciliation**: Catch discrepancies early
2. **Monitor DLQ daily**: Investigate error patterns
3. **Review dashboards weekly**: Identify trends
4. **Backup offsets daily**: Enable quick recovery
5. **Rotate logs regularly**: Prevent disk space issues

### Performance

1. **Tune batch sizes**: Balance latency vs throughput
2. **Scale horizontally**: Add workers before vertical scaling
3. **Monitor lag closely**: Alert before it becomes critical
4. **Optimize PostgreSQL**: Indexes, vacuuming, connection pooling
5. **Use connection pooling**: Reduce connection overhead

---

## Appendix

### Useful Commands

```bash
# Quick health check
./scripts/health-check.sh

# Check connector status
curl -s http://localhost:8083/connectors/scylla-source/status | jq .

# Restart connector
curl -X POST http://localhost:8083/connectors/scylla-source/restart

# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe consumer group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-postgres-sink

# Check DLQ messages
./scripts/check-dlq.sh

# Run reconciliation
./scripts/reconcile.py full --table users --dry-run

# Generate load for testing
./scripts/generate-load.sh --duration 60 --rate 100
```

### Configuration Files

- `configs/connectors/scylla-source.json` - Source connector config
- `configs/connectors/postgres-sink.json` - Sink connector config
- `docker-compose.yml` - Service orchestration
- `configs/kafka-connect/worker.properties` - Worker configuration
- `docker/prometheus/alert-rules.yml` - Alert definitions

### Related Documentation

- [Architecture](architecture.md) - System architecture and design
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [Scaling](scaling.md) - Capacity planning and scaling guide
- [README](../README.md) - Project overview and quickstart

---

**Document Version**: 1.0
**Last Updated**: 2025-12-09
**Maintained By**: DevOps Team
**Review Cycle**: Monthly
