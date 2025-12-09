<!--
Sync Impact Report
==================
Version change: 0.0.0 → 1.0.0
Modified principles: All principles created (initial version)
Added sections:
  - All core principles (I-VII)
  - Data Pipeline Architecture section
  - Development Workflow section
  - Governance section
Removed sections: None
Templates requiring updates:
  ✅ Updated: plan-template.md (Constitution Check section aligned)
  ✅ Updated: spec-template.md (Requirements aligned with principles)
  ✅ Updated: tasks-template.md (Task phases aligned with principles)
Follow-up TODOs: None - all placeholders filled
-->

# Scylla-PG-CDC Constitution

## Core Principles

### I. Exactly-Once Semantics (NON-NEGOTIABLE)

CDC pipelines MUST guarantee exactly-once delivery semantics. This means:
- Every change event is processed exactly once, never duplicated or lost
- Idempotent operations required at every stage
- Offset/checkpoint management is mandatory with atomic commits
- State recovery procedures defined for all failure scenarios

**Rationale**: CDC pipelines are critical data infrastructure. Data loss or duplication can corrupt downstream systems, cause financial discrepancies, or violate compliance requirements. Eventual consistency is insufficient for change data capture use cases.

### II. Schema Evolution & Backward Compatibility

All schema changes MUST be backward compatible. The system MUST handle:
- Column additions/removals without breaking existing consumers
- Type changes with explicit migration paths
- Version negotiation between source and sink
- Schema registry integration for validation

**Rationale**: CDC systems operate in production 24/7 with multiple consumers. Breaking changes cause cascading failures across dependent systems. Forward and backward compatibility enables zero-downtime deployments and gradual rollouts.

### III. Observable Pipelines (NON-NEGOTIABLE)

Every CDC pipeline component MUST be thoroughly observable:
- Structured logging with correlation IDs for tracing events end-to-end
- Metrics exposed: lag, throughput, error rates, checkpoint age
- Distributed tracing integration for cross-system visibility
- Health checks and readiness probes at every component

**Rationale**: CDC failures are often discovered late due to asynchronous processing. Without observability, debugging requires log archaeology across multiple systems. Rich metrics enable proactive alerting before data loss occurs.

### IV. Failure Isolation & Graceful Degradation

Component failures MUST NOT cascade. Design for resilience:
- Circuit breakers on external dependencies (database, message queue, sinks)
- Retry with exponential backoff and jitter
- Dead letter queues for poison messages
- Bulkheads isolating failure domains (per-table, per-partition)

**Rationale**: CDC systems integrate numerous failure-prone components (networks, databases, message brokers). A single component failure should degrade gracefully, not bring down the entire pipeline. Isolating failures preserves partial availability.

### V. Test-First Development (NON-NEGOTIABLE)

All CDC logic MUST be developed test-first following TDD:
- Unit tests written before implementation, verified to FAIL first
- Integration tests for source capture → sink delivery flows
- Contract tests for schema compatibility across versions
- Chaos engineering tests simulating failures (network partitions, process crashes)

**Rationale**: CDC correctness bugs are catastrophic and hard to detect in production. Test-first development forces thinking about edge cases (late-arriving data, out-of-order events, duplicates) upfront. Integration tests validate the end-to-end guarantee that unit tests cannot.

### VI. Stateless Processing with Externalized State

Processing logic MUST be stateless with checkpoints externalized:
- No local state that cannot be reconstructed from checkpoints
- Checkpoints stored durably in external storage (database, object store)
- Processing restarts from last committed checkpoint without data loss
- Horizontal scaling through partitioning (per table, per key range)

**Rationale**: Stateful services complicate scaling and recovery. Externalizing state to durable storage enables seamless failover, elastic scaling, and simplified operations. Any node can process any partition after loading checkpoint state.

### VII. Performance Profiling & Capacity Planning

CDC systems MUST define and monitor performance SLIs/SLOs:
- Maximum tolerable lag defined per use case (e.g., <5s for real-time, <1h for batch)
- Throughput requirements specified (events/sec, GB/day)
- Resource limits and backpressure mechanisms
- Regular load testing and capacity planning reviews

**Rationale**: CDC performance directly impacts downstream systems' freshness. Unbounded lag can cause cascading failures, storage exhaustion, or SLA violations. Proactive monitoring and capacity planning prevent surprise outages.

## Data Pipeline Architecture

CDC pipelines follow a modular, composable architecture:

### Pipeline Stages

1. **Source Capture**: Extract change events from source database (Postgres, Scylla)
   - Use native CDC mechanisms (logical replication, commit log reading)
   - Handle schema discovery and evolution
   - Implement checkpointing at source level

2. **Transform & Enrich**: Process events before delivery
   - Filtering, projection, aggregation
   - Enrichment with reference data
   - Format conversion (row → message format)

3. **Sink Delivery**: Deliver to downstream systems
   - Support multiple sink types (Kafka, database, API)
   - Batch delivery for efficiency where latency allows
   - Sink-specific error handling and retries

### Operational Requirements

- Each stage deployable independently
- Configuration externalized (no hardcoded connection strings)
- Deployment automation with rollback capability
- Monitoring dashboards for each pipeline

## Development Workflow

### Code Quality Standards

- **Linting**: All code passes linter checks (no warnings tolerated)
- **Type Safety**: Strong typing enforced (TypeScript strict mode, Python type hints)
- **Code Review**: All changes require peer review, constitution compliance verification
- **Documentation**: Public APIs documented with examples, runbooks for operational procedures

### Testing Requirements

- **Unit Test Coverage**: Minimum 80% line coverage for business logic
- **Integration Tests**: End-to-end flows validated in test environment
- **Performance Tests**: Benchmarks for critical paths (event processing latency)
- **Chaos Tests**: Failure injection tests run regularly (weekly minimum)

### Complexity Constraints

- **YAGNI Principle**: Implement only what is needed now, not speculative future requirements
- **Simplicity First**: Choose simple solutions over clever ones unless performance requires optimization
- **Justified Complexity**: Any abstraction, pattern, or framework must solve an actual problem documented in plan.md

## Governance

This constitution supersedes all other development practices and guidelines. Changes to these principles require:

1. **Proposal**: Document proposed change with rationale
2. **Review**: Team review and impact analysis on existing code
3. **Approval**: Unanimous consent or 2/3 majority vote
4. **Migration Plan**: Plan for bringing existing code into compliance

### Enforcement

- All pull requests MUST verify compliance with these principles
- Constitution checks integrated into plan.md and tasks.md workflows
- Regular audits (quarterly) to verify adherence
- Violations documented in plan.md Complexity Tracking section with justification

### Version Control

Version numbers follow semantic versioning:
- **MAJOR**: Backward-incompatible principle changes or removals
- **MINOR**: New principles added or existing principles materially expanded
- **PATCH**: Clarifications, typo fixes, non-semantic improvements

**Version**: 1.0.0 | **Ratified**: 2025-12-09 | **Last Amended**: 2025-12-09
