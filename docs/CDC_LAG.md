# CDC Pipeline Latency Analysis

**Date:** 2025-12-17
**Pipeline:** ScyllaDB → Kafka Connect → PostgreSQL
**Analysis Type:** Production Readiness Assessment

---

## Executive Summary

**Current State:** The CDC pipeline exhibits **30-60 second replication latency** due to aggressive batching configurations optimized for throughput rather than latency.

**Production Viability:** ❌ **NOT SUITABLE** for real-time operational use cases. Acceptable only for batch analytics or non-time-sensitive data warehousing.

**Recommended Action:** Implement Phase 1 tuning (see recommendations) to achieve **10-15 second latency** for production deployment.

---

## 1. Would This Time Lag Cause Bottlenecks in Production?

### Answer: **YES** - For most production scenarios requiring near real-time data

### Current Performance Metrics

| Metric | Value | Status |
|--------|-------|--------|
| **Average Replication Latency** | 30-60 seconds | ⚠️ High |
| **Test Verification Wait Time** | 60 seconds (required) | ⚠️ High |
| **Current Consumer Lag** | 26 messages | ⚠️ Accumulating |
| **Write Pattern** | Irregular (30-120s gaps) | ⚠️ Unpredictable |
| **Batch Flush Behavior** | Non-deterministic | ❌ No guarantees |

### Root Cause Analysis

The latency is caused by **three compounding factors**:

#### 1. Aggressive Sink Batch Size
```json
{
  "batch.size": 3000,
  "consumer.override.max.poll.records": 1000
}
```

**Problem:** The sink connector waits to accumulate **3,000 records** before writing to PostgreSQL. In low-volume scenarios, this creates indefinite delays with no time-based flush guarantee.

**Evidence:**
```
Write timestamps (last hour):
13:48:36 → 13:51:06 (150s gap)
13:51:06 → 13:52:06 (60s gap)
13:52:06 → 13:54:36 (150s gap)
14:09:06 → 14:13:36 (270s gap)
```

#### 2. Source Heartbeat Interval
```json
{
  "heartbeat.interval.ms": 30000,
  "poll.interval.ms": 1000
}
```

**Problem:** The source connector emits heartbeats every **30 seconds**, which drives the minimum practical flush interval for the entire pipeline. Even though polling happens every 1 second, actual CDC log stream commits are tied to heartbeat timing.

#### 3. No Explicit Flush Timeout

**Problem:** The configuration lacks explicit time-based flush controls (no `flush.size`, `linger.ms`, or equivalent). The pipeline relies entirely on:
- Batch size accumulation (3000 records), OR
- Implicit Kafka Connect commit intervals

This creates **non-deterministic latency** that varies based on throughput.

### Production Impact Assessment

#### ❌ **PROBLEMATIC Use Cases:**

| Use Case | Impact | Risk Level |
|----------|--------|------------|
| **Real-time Analytics Dashboards** | Stale data, poor UX | HIGH |
| **Fraud Detection Systems** | Delayed alerts, security risk | CRITICAL |
| **Inventory Management** | Outdated stock levels, overselling | HIGH |
| **User Profile Updates** | Changes not reflected, confusing UX | MEDIUM |
| **Event-Driven Architectures** | Cascade delays, SLA breaches | HIGH |
| **Order Processing** | Payment/fulfillment delays | HIGH |
| **Compliance/Audit Trails** | Time-sensitive requirements unmet | MEDIUM |

#### ✅ **ACCEPTABLE Use Cases:**

| Use Case | Notes |
|----------|-------|
| **Batch Analytics (Hourly/Daily)** | 60s delay negligible |
| **Historical Data Warehousing** | Non-real-time by design |
| **Cold Storage/Archival** | Time insensitive |
| **Read Replica (Non-critical)** | Eventual consistency acceptable |
| **Reporting/BI (Non-live)** | Scheduled jobs unaffected |

### Operational Risks

1. **Consumer Lag Accumulation:**
   - Current lag: 26 messages and growing
   - No alerting on lag thresholds
   - Risk of falling behind during traffic spikes

2. **Unpredictable Latency:**
   - Varies from 30-270 seconds based on volume
   - No SLA can be guaranteed
   - Difficult to troubleshoot "slow data" issues

3. **Testing Challenges:**
   - Required 60-second wait in test scripts
   - Slows down CI/CD pipelines
   - Makes debugging difficult

---

## 2. Options for Reducing Time Lag

### Configuration Reference

**Current Connector Settings:**

**Source Connector (ScyllaDB):**
```json
{
  "poll.interval.ms": 1000,
  "heartbeat.interval.ms": 30000,
  "max.batch.size": 2048,
  "max.queue.size": 8192
}
```

**Sink Connector (PostgreSQL):**
```json
{
  "batch.size": 3000,
  "poll.interval.ms": 50,
  "consumer.override.max.poll.records": 1000,
  "consumer.override.session.timeout.ms": 30000,
  "consumer.override.heartbeat.interval.ms": 10000
}
```

---

### Option 1: Reduce Sink Batch Size ⭐ **RECOMMENDED**

**Priority:** HIGH
**Risk Level:** LOW
**Effort:** Minimal (config change)

**Configuration Changes:**
```json
{
  "batch.size": 200,                              // Current: 3000
  "consumer.override.max.poll.records": 200       // Current: 1000
}
```

**Expected Impact:**
- **Latency Reduction:** 30-60s → 10-15s
- **Trade-off:** Slightly more database write operations
- **Database Impact:** Minimal (200 records per batch still efficient)

**Rationale:**
- Batch size of 200 balances latency and throughput
- Most production systems generate > 200 records per 10s
- PostgreSQL handles small batches efficiently with JDBC connection pooling

**Risk Assessment:**
- ✅ Proven pattern in production Kafka Connect deployments
- ✅ No architectural changes required
- ⚠️ Monitor database CPU/IO after deployment

---

### Option 2: Reduce Source Heartbeat Interval ⭐ **RECOMMENDED**

**Priority:** HIGH
**Risk Level:** LOW
**Effort:** Minimal (config change)

**Configuration Changes:**
```json
{
  "heartbeat.interval.ms": 10000,    // Current: 30000
  "poll.interval.ms": 500            // Current: 1000 (optional refinement)
}
```

**Expected Impact:**
- **Latency Reduction:** Minimum flush interval 30s → 10s
- **Overhead:** Negligible (heartbeat messages are small)
- **Pipeline Consistency:** More frequent commits, better recovery

**Rationale:**
- Heartbeats drive the CDC log stream cursor advancement
- 10-second interval is standard for operational CDC pipelines
- Improves failure recovery and reduces data loss window

**Risk Assessment:**
- ✅ No impact on data correctness
- ✅ Standard practice for production CDC
- ✅ Minimal network overhead

---

### Option 3: Add Explicit Flush Controls

**Priority:** MEDIUM
**Risk Level:** LOW
**Effort:** Moderate (requires validation)

**Configuration Changes:**
```json
{
  "consumer.override.auto.commit.interval.ms": 5000,  // Add explicit commit interval
  "flush.size": 100                                    // If supported by connector version
}
```

**Expected Impact:**
- **Latency Guarantee:** Maximum 5-second delay
- **Predictability:** Time-based SLA possible

**Rationale:**
- Provides deterministic upper bound on latency
- Decouples flush timing from batch accumulation

**Risk Assessment:**
- ⚠️ Connector version compatibility must be verified
- ⚠️ May interact with existing batching logic
- ✅ Improves operational predictability

**Implementation Notes:**
- Check Confluent JDBC Sink Connector documentation for `flush.size` support
- Test in staging environment first
- Monitor for flush timing in connector logs

---

### Option 4: Increase Task Parallelism

**Priority:** MEDIUM
**Risk Level:** MEDIUM
**Effort:** Moderate (requires resource scaling)

**Configuration Changes:**
```json
{
  "tasks.max": 8,                                 // Current: 4
  "max.batch.size": 1024,                         // Source: Current: 2048
  "consumer.override.max.poll.records": 500       // Sink: Reduce per-task batch
}
```

**Expected Impact:**
- **Throughput:** 2x increase in parallel processing
- **Lag Reduction:** Better handling of traffic spikes
- **Latency:** Marginal improvement (not primary benefit)

**Rationale:**
- More tasks = more concurrent database connections
- Better distribution across Kafka topic partitions
- Reduces risk of lag accumulation under load

**Risk Assessment:**
- ⚠️ Requires more CPU/memory resources
- ⚠️ PostgreSQL connection pool must support more connections
- ⚠️ Kafka topic must have sufficient partitions (currently 8 per topic)
- ✅ Improves overall pipeline resilience

**Prerequisites:**
```sql
-- Verify PostgreSQL can handle more connections
SHOW max_connections;  -- Should be > 100

-- Check Kafka topic partition count
kafka-topics --describe --topic scylla-cluster.app_data.users
```

---

### Option 5: Streaming Mode (Ultra-Low Latency)

**Priority:** LOW (Only for critical use cases)
**Risk Level:** HIGH
**Effort:** High (significant tuning required)

**Configuration Changes:**
```json
// Sink Connector
{
  "batch.size": 1,                              // Write every record immediately
  "poll.interval.ms": 100,
  "consumer.override.max.poll.records": 1
}

// Source Connector
{
  "heartbeat.interval.ms": 1000,
  "poll.interval.ms": 100,
  "max.batch.size": 1
}
```

**Expected Impact:**
- **Latency:** 1-5 seconds (near real-time)
- **Write Operations:** 100-1000x increase
- **Resource Cost:** 5-10x higher CPU, network, I/O

**Rationale:**
- Every CDC event triggers immediate write
- No batching delays
- Maximum responsiveness

**Risk Assessment:**
- ❌ May overwhelm PostgreSQL with small writes
- ❌ Significantly higher cost per record
- ❌ Requires extensive database tuning:
  - WAL settings optimization
  - Connection pooling (pgBouncer)
  - `synchronous_commit = off` consideration
  - Index tuning for high write rate
- ⚠️ Only viable for:
  - Latency-critical tables (< 1000 ops/sec)
  - High-value transactions (fraud detection)
  - Systems with dedicated database capacity

**Database Tuning Required:**
```sql
-- PostgreSQL optimization for streaming mode
ALTER SYSTEM SET shared_buffers = '4GB';
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET max_wal_size = '4GB';
ALTER SYSTEM SET synchronous_commit = 'off';  -- Trades durability for speed
```

---

### Option 6: Hybrid Architecture (Strategic Approach)

**Priority:** LOW (Architectural decision)
**Risk Level:** HIGH
**Effort:** High (multi-week implementation)

**Architecture:**
```
┌─────────────────────────────────────────────────┐
│  ScyllaDB Application Layer                     │
└────────┬──────────────────────┬─────────────────┘
         │                      │
         │ Write                │ Write
         ▼                      ▼
    ┌─────────┐          ┌──────────────┐
    │ ScyllaDB│          │ PostgreSQL   │ ◄── Fast Path (Direct Write)
    └────┬────┘          │ (Critical    │     • Sub-second latency
         │               │  Tables)     │     • Trades consistency for speed
         │ CDC           └──────────────┘
         ▼
    ┌─────────────┐
    │ Kafka CDC   │
    │ Pipeline    │
    └─────┬───────┘
          │
          ▼
    ┌──────────────┐
    │ PostgreSQL   │ ◄── Slow Path (CDC)
    │ (All Tables) │     • Audit trail
    │              │     • Reconciliation
    └──────────────┘     • Historical data
```

**Implementation:**
- Critical tables: Dual-write to both ScyllaDB and PostgreSQL
- Non-critical tables: CDC pipeline only
- Periodic reconciliation job to detect divergence

**Expected Impact:**
- **Critical Path Latency:** < 1 second (direct writes)
- **Audit Trail:** Complete via CDC (eventual consistency)
- **Consistency Guarantee:** Reconciliation detects/fixes drift

**Risk Assessment:**
- ❌ Complex architecture, harder to maintain
- ❌ Application changes required
- ❌ Risk of write failures causing inconsistency
- ⚠️ Requires robust error handling and monitoring
- ✅ Provides best of both worlds (speed + audit)

**Use Case Fit:**
- Financial transactions requiring immediate confirmation
- User-facing features needing instant updates
- Hybrid OLTP/OLAP workloads

---

## Recommended Implementation Plan

### Phase 1: Quick Win (Immediate - 1 Hour) ⭐

**Goal:** Achieve 10-15 second latency with minimal risk

**Changes:**
```json
// File: docker/kafka-connect/connectors/postgres-sink.json
{
  "batch.size": 200,                              // Changed from 3000
  "consumer.override.max.poll.records": 200       // Changed from 1000
}

// File: docker/kafka-connect/connectors/scylla-source.json.template
{
  "heartbeat.interval.ms": 10000                  // Changed from 30000
}
```

**Validation Steps:**
1. Update connector configurations
2. Redeploy connectors: `make cleanup-connectors && make deploy-connectors`
3. Run test: `make test-replication`
4. Adjust test wait time to 20 seconds in Makefile
5. Monitor consumer lag for 24 hours

**Success Criteria:**
- ✅ Test passes with 15-second wait time
- ✅ Consumer lag remains < 10 messages
- ✅ No errors in connector logs

---

### Phase 2: Monitoring & Fine-Tuning (Week 1-2)

**Goal:** Establish operational baselines and optimize

**Actions:**
1. **Implement Lag Monitoring:**
   ```bash
   # Add to scripts/monitor-consumer-lag.sh
   docker exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe --group connect-postgres-jdbc-sink \
     | awk '{sum+=$6} END {print sum}'
   ```

2. **Set Up Alerting:**
   - Alert if lag > 100 messages for 5 minutes
   - Alert if latency > 30 seconds (sample-based testing)

3. **Traffic Pattern Analysis:**
   - Monitor actual throughput (records/second)
   - Identify peak vs. off-peak patterns
   - Adjust `batch.size` if needed:
     - Low traffic (< 50 rec/sec): reduce to 100
     - High traffic (> 500 rec/sec): increase to 500

4. **Database Performance:**
   - Monitor PostgreSQL write latency
   - Check connection pool utilization
   - Verify no lock contention on target tables

**Tuning Guidelines:**
| Traffic Profile | Batch Size | Expected Latency |
|-----------------|------------|------------------|
| Low (< 50/sec) | 100 | 5-10s |
| Medium (50-500/sec) | 200-300 | 10-15s |
| High (> 500/sec) | 500-1000 | 15-30s |

---

### Phase 3: Capacity Planning (Month 1)

**Goal:** Ensure pipeline can scale with growth

**Assessment Tasks:**

1. **Load Testing:**
   - Simulate 10x current traffic
   - Measure latency under load
   - Identify bottlenecks (CPU, network, disk I/O)

2. **Failure Scenarios:**
   - Test connector restarts (measure recovery time)
   - Test PostgreSQL failover
   - Test network partition handling

3. **Scalability Review:**
   - Evaluate need for Option 4 (more tasks)
   - Consider Kafka topic partition increase
   - Review database sharding strategy

4. **Cost Analysis:**
   - Calculate cost per million records
   - Compare batch vs. streaming mode costs
   - Justify infrastructure investments

---

### Phase 4: Architecture Review (If 15s Still Too Slow)

**Triggers for Phase 4:**
- Business requirement for < 5s latency
- Specific critical tables need real-time updates
- Regulatory compliance requires immediate replication

**Decision Tree:**
```
Is latency requirement < 5 seconds?
├─ NO  → Stay with Phase 1 configuration
└─ YES → Is it ALL tables or SPECIFIC tables?
          ├─ ALL tables → Implement Option 5 (Streaming Mode)
          │              Requires database capacity upgrade
          │
          └─ SPECIFIC tables → Implement Option 6 (Hybrid Architecture)
                                Dual-write critical tables only
```

---

## Comparison Matrix

| Configuration | Latency | Throughput | DB Load | Complexity | Cost | Recommendation |
|---------------|---------|------------|---------|------------|------|----------------|
| **Current** | 30-60s | High | Low | Low | Low | ❌ Not production ready |
| **Phase 1 (Optimized)** | 10-15s | High | Medium | Low | Low | ✅ **START HERE** |
| **Streaming Mode** | 1-5s | Medium | Very High | Medium | High | ⚠️ Critical tables only |
| **Hybrid Architecture** | < 1s | High | High | High | High | ⚠️ Strategic use cases |

---

## Test Script Updates Required

After implementing Phase 1, update test scripts:

```bash
# File: Makefile (line 85)
# Change from:
@./scripts/test-replication.sh --table users --wait 60

# To:
@./scripts/test-replication.sh --table users --wait 20
```

This validates that the 10-15 second latency goal is met.

---

## Monitoring & Alerting Checklist

### Key Metrics to Track

1. **Consumer Lag**
   ```bash
   # Current value
   docker exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe --group connect-postgres-jdbc-sink
   ```
   - Alert: > 100 messages for 5+ minutes

2. **Replication Latency**
   - Measure: Insert timestamp in ScyllaDB vs. arrival in PostgreSQL
   - Alert: > 30 seconds (99th percentile)

3. **Connector Health**
   ```bash
   curl http://localhost:8083/connectors/postgres-jdbc-sink/status
   ```
   - Alert: Any task in FAILED state

4. **Database Write Rate**
   ```sql
   SELECT schemaname, relname,
          n_tup_ins AS inserts,
          n_tup_upd AS updates
   FROM pg_stat_user_tables
   WHERE schemaname = 'cdc_data';
   ```
   - Track: Records/second trend

### Grafana Dashboard (Recommended)

Create dashboard with panels:
- Consumer lag over time (line chart)
- Write throughput by table (stacked area)
- Connector task status (stat panel)
- Latency percentiles (heatmap)

---

## Rollback Plan

If Phase 1 changes cause issues:

```bash
# 1. Revert connector configurations
git checkout HEAD -- docker/kafka-connect/connectors/

# 2. Redeploy original configuration
make cleanup-connectors
make deploy-connectors

# 3. Verify system returns to baseline
make test-replication  # Should pass with 60s wait
```

**Rollback Triggers:**
- Consumer lag grows unbounded
- Database CPU > 80% sustained
- Application errors related to data freshness
- Connector tasks fail repeatedly

---

## Conclusion

**Current State:** The CDC pipeline is configured for high-throughput batch processing, resulting in 30-60 second latency that is **unacceptable for real-time production use**.

**Immediate Action:** Implement **Phase 1 recommendations** to achieve 10-15 second latency with minimal risk and effort. This involves reducing batch size from 3000 to 200 and heartbeat interval from 30s to 10s.

**Long-Term Strategy:** Monitor performance, establish baselines, and consider advanced options (streaming mode, hybrid architecture) only if business requirements justify the additional complexity and cost.

**Success Metrics:**
- ✅ Latency: < 15 seconds (99th percentile)
- ✅ Consumer lag: < 50 messages sustained
- ✅ Throughput: > 1000 records/second capacity
- ✅ Zero data loss or corruption

---

## References

- [Confluent JDBC Sink Connector Documentation](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html)
- [Kafka Connect Performance Tuning](https://docs.confluent.io/platform/current/connect/references/performance-tuning.html)
- [ScyllaDB CDC Documentation](https://opensource.docs.scylladb.com/stable/using-scylla/cdc/)
- Consumer Lag Monitoring: `scripts/monitor-consumer-lag.sh`
- Test Replication: `make test-replication`
- Connector Status: `make monitor`

---

**Document Version:** 1.0
**Last Updated:** 2025-12-17
**Next Review:** After Phase 1 implementation (2025-12-18)
