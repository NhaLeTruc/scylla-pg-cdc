# Feature Specification: ScyllaDB to Postgres CDC Pipeline

**Feature Branch**: `001-scylla-pg-cdc`
**Created**: 2025-12-09
**Status**: Draft
**Input**: User description: "Create a change data capture pipeline from a ScyllaDB to a Postgres data-warehouse. The pipeline MUST has the following qualities: 1. Locally testable. Its docker compose environment MUST enables e2e, and integration tests locally. 2. Communities supported. It MUST utilize free open-sourced softwares, and minimum amount of custom code. 3. Observable. There MUST BE proper logs management systems and monitoring infrastructures. 4. Strictly Tested. Tests MUST be written first before implementation for all of its components. 5. Robust. There MUST be proper reconciliation mechanism, error handling, retry strategies, and stale events handling for the cdc pipeline. 6. Flexible. There MUST be proper handlings of schema evolutions, and dirty data. 7. Secured. There MUST be a proper safe-guards against SQL injection and other commom security vulnerabilities. 8. Efficiently Tested. DO NOT WRITE ANY TEST until the main codebase has been completely implemented."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Basic Change Capture & Replication (Priority: P1)

As a data engineer, I need real-time change events from ScyllaDB to flow into Postgres data warehouse so that downstream analytics and reporting have access to near-real-time operational data.

**Why this priority**: This is the core functionality. Without basic capture and delivery, no other features matter. This provides immediate value by enabling real-time data synchronization.

**Independent Test**: Can be fully tested by inserting a row into ScyllaDB, waiting for pipeline processing, and querying Postgres to verify the row appears with correct data. Delivers a working end-to-end data pipeline.

**Acceptance Scenarios**:

1. **Given** ScyllaDB contains a table with data, **When** a new row is inserted, **Then** the row appears in Postgres data warehouse within the defined SLA timeframe
2. **Given** ScyllaDB row is updated, **When** the CDC pipeline processes the change, **Then** the corresponding Postgres row reflects the updated values
3. **Given** ScyllaDB row is deleted, **When** the CDC pipeline processes the deletion, **Then** the corresponding Postgres row is marked as deleted or removed per configuration
4. **Given** multiple changes occur rapidly, **When** the pipeline processes them, **Then** all changes are captured without data loss

---

### User Story 2 - Schema Evolution Handling (Priority: P2)

As a data engineer, I need the pipeline to handle schema changes gracefully so that adding columns or modifying table structures doesn't break the replication process or require manual intervention.

**Why this priority**: Schema evolution is inevitable in production systems. Without this, every schema change becomes a pipeline outage requiring manual fixes.

**Independent Test**: Can be tested by adding a new column to a ScyllaDB table, inserting data with the new column, and verifying Postgres receives the data with the new schema without pipeline failure.

**Acceptance Scenarios**:

1. **Given** a ScyllaDB table schema, **When** a new column is added, **Then** the pipeline detects the schema change and continues replication including the new column
2. **Given** a column is removed from ScyllaDB, **When** CDC events are processed, **Then** the pipeline handles the missing column gracefully without failing
3. **Given** a column type changes compatibly, **When** data flows through pipeline, **Then** values are transformed appropriately for Postgres
4. **Given** incompatible schema changes occur, **When** the pipeline detects them, **Then** clear error messages are logged and alerts are triggered

---

### User Story 3 - Failure Recovery & Reconciliation (Priority: P2)

As a data engineer, I need the pipeline to recover from failures and reconcile any data inconsistencies so that temporary outages don't result in permanent data loss or corruption.

**Why this priority**: Production systems fail. Networks partition, services restart, databases go offline. Without robust recovery, data integrity is compromised.

**Independent Test**: Can be tested by intentionally stopping the pipeline mid-processing, making changes to ScyllaDB, restarting the pipeline, and verifying all changes are eventually replicated correctly.

**Acceptance Scenarios**:

1. **Given** the pipeline crashes mid-processing, **When** it restarts, **Then** it resumes from the last committed checkpoint without data loss or duplication
2. **Given** Postgres is temporarily unavailable, **When** the pipeline attempts delivery, **Then** changes are retried with exponential backoff until successful
3. **Given** data inconsistencies are detected, **When** reconciliation runs, **Then** discrepancies are identified and corrected
4. **Given** poison messages that cause processing failures, **When** retry limits are exceeded, **Then** messages are moved to dead letter queue for investigation

---

### User Story 4 - Observability & Monitoring (Priority: P3)

As a platform engineer, I need comprehensive monitoring and logging so that I can proactively detect issues, debug failures, and ensure the pipeline meets SLAs.

**Why this priority**: Without observability, issues are discovered by downstream users reporting stale data. This enables proactive monitoring and faster incident response.

**Independent Test**: Can be tested by running the pipeline, generating various events and errors, and verifying that metrics dashboards show accurate lag/throughput/error rates and logs contain correlation IDs for tracing.

**Acceptance Scenarios**:

1. **Given** the pipeline is processing events, **When** metrics are queried, **Then** current lag, throughput, and error rates are accurately displayed
2. **Given** an error occurs during processing, **When** logs are reviewed, **Then** structured logs with correlation IDs enable tracing the event end-to-end
3. **Given** pipeline health checks are configured, **When** components become unhealthy, **Then** health endpoints report degraded status
4. **Given** SLA thresholds are exceeded, **When** monitored continuously, **Then** alerts are triggered to notify operators

---

### User Story 5 - Local Development & Testing Environment (Priority: P1)

As a developer, I need a local Docker Compose environment that replicates the full production pipeline so that I can develop, test, and debug changes without requiring access to production infrastructure.

**Why this priority**: This is foundational for development velocity. Without local testing, every change requires deployment to shared environments, slowing iteration cycles.

**Independent Test**: Can be tested by running `docker-compose up`, executing integration tests locally, and verifying all tests pass without external dependencies.

**Acceptance Scenarios**:

1. **Given** Docker and Docker Compose are installed, **When** `docker-compose up` is executed, **Then** full environment (ScyllaDB, Postgres, pipeline, monitoring) starts successfully
2. **Given** the local environment is running, **When** integration tests execute, **Then** they complete successfully with all components communicating properly
3. **Given** changes are made to pipeline code, **When** containers are rebuilt, **Then** changes are reflected in the running environment
4. **Given** test data is seeded, **When** end-to-end tests run, **Then** data flows from ScyllaDB through the pipeline to Postgres as expected

---

### Edge Cases

- What happens when ScyllaDB generates events faster than the pipeline can process (backpressure)?
- How does the system handle very large change events (e.g., multi-MB blob columns)?
- What happens when Postgres and ScyllaDB have conflicting data types for the same logical column?
- How does the pipeline handle ScyllaDB cluster topology changes (nodes added/removed)?
- What happens when clock skew exists between ScyllaDB nodes and pipeline components?
- How does the system handle duplicate events from source CDC mechanism?
- What happens when Postgres runs out of storage space during replication?
- How does the pipeline handle tables with millions of columns or deeply nested data structures?
- What happens when network partitions occur between pipeline components?
- How does the system handle malformed or corrupted CDC events from ScyllaDB?

## Requirements *(mandatory)*

### Functional Requirements

#### Core Replication

- **FR-001**: System MUST capture INSERT, UPDATE, and DELETE operations from ScyllaDB tables in near real-time
- **FR-002**: System MUST deliver captured changes to Postgres data warehouse with exactly-once semantics (no duplicates, no data loss)
- **FR-003**: System MUST maintain change event ordering per primary key to ensure consistency
- **FR-004**: System MUST support replication of multiple ScyllaDB tables to corresponding Postgres tables
- **FR-005**: System MUST checkpoint progress durably so processing can resume after failures without data loss

#### Schema Management

- **FR-006**: System MUST detect schema changes (column additions, removals, type changes) automatically
- **FR-007**: System MUST handle backward-compatible schema evolution without manual intervention
- **FR-008**: System MUST validate schema compatibility between source and sink before processing changes
- **FR-009**: System MUST log schema change events with before/after schema snapshots
- **FR-010**: System MUST handle dirty data (null values in non-null columns, invalid types) with configurable error handling policies

#### Resilience & Recovery

- **FR-011**: System MUST implement retry logic with exponential backoff and jitter for transient failures
- **FR-012**: System MUST implement circuit breakers to prevent cascading failures when dependencies are unhealthy
- **FR-013**: System MUST move poison messages to dead letter queue after exceeding retry threshold
- **FR-014**: System MUST support data reconciliation to detect and correct inconsistencies between source and sink
- **FR-015**: System MUST handle stale events (events delayed significantly from commit time) with configurable policies

#### Observability

- **FR-016**: System MUST emit metrics for lag (time between event commit and delivery), throughput (events/sec), and error rates
- **FR-017**: System MUST produce structured logs with correlation IDs for tracing events end-to-end
- **FR-018**: System MUST expose health check endpoints reporting component readiness and liveness
- **FR-019**: System MUST integrate with monitoring dashboards visualizing pipeline performance
- **FR-020**: System MUST emit alerts when SLA thresholds (lag, error rate) are breached

#### Security

- **FR-021**: System MUST use parameterized queries or prepared statements to prevent SQL injection
- **FR-022**: System MUST encrypt connections to ScyllaDB and Postgres using TLS
- **FR-023**: System MUST authenticate to data sources using credentials stored securely (not hardcoded)
- **FR-024**: System MUST implement principle of least privilege for database permissions
- **FR-025**: System MUST sanitize and validate all data before writing to Postgres

#### Development & Testing

- **FR-026**: System MUST provide Docker Compose environment for local end-to-end testing
- **FR-027**: System MUST support integration tests executable locally without external dependencies
- **FR-028**: System MUST use open-source, community-supported components with minimal custom code
- **FR-029**: System MUST include test data fixtures for seeding development environments
- **FR-030**: System MUST follow hybrid testing approach where unit tests for core business logic are written before implementation (TDD), while integration tests and end-to-end tests are written after component implementation is complete

### Key Entities

- **Change Event**: Represents a single data modification (INSERT/UPDATE/DELETE) from ScyllaDB, containing operation type, table name, primary key, before/after values, commit timestamp, and correlation ID
- **Checkpoint**: Tracks pipeline processing progress per table/partition, containing last processed offset/timestamp and commit status
- **Schema Version**: Captures table schema at a point in time, including column names, types, constraints, and version number
- **Dead Letter Message**: Failed change event that exceeded retry threshold, containing original event, error details, retry count, and failure timestamp
- **Reconciliation Record**: Result of comparing source and sink data, identifying missing, inconsistent, or extra rows with discrepancy details

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Change events are delivered from ScyllaDB to Postgres within 5 seconds at p95 latency during normal operation
- **SC-002**: System processes at least 10,000 change events per second without degradation
- **SC-003**: Zero data loss or duplication occurs during 7-day continuous operation under load testing
- **SC-004**: Pipeline recovers from component failures (database restart, network partition) within 30 seconds and resumes processing without manual intervention
- **SC-005**: Schema changes are detected and handled automatically without pipeline downtime in 95% of cases
- **SC-006**: All logs include correlation IDs enabling end-to-end tracing of any event within 2 minutes
- **SC-007**: Monitoring dashboards accurately reflect pipeline health within 10 seconds of status changes
- **SC-008**: Local Docker Compose environment starts successfully and passes all integration tests in under 5 minutes
- **SC-009**: 90% of pipeline code consists of open-source components with less than 10% custom glue code
- **SC-010**: Zero SQL injection vulnerabilities detected in security testing and code reviews

### Assumptions

- ScyllaDB version supports CDC capabilities (3.1+) with commit log reading or similar mechanism
- Postgres version supports the data types present in ScyllaDB tables (12+)
- Network bandwidth between pipeline and databases is sufficient for expected throughput (at least 100 Mbps)
- ScyllaDB tables have primary keys defined (required for tracking changes and ensuring ordering)
- Change events from ScyllaDB are available for at least 24 hours before expiring
- Pipeline can tolerate eventual consistency (changes appear in Postgres within seconds, not milliseconds)
- Docker and Docker Compose are available for development environments
- Monitoring infrastructure (metrics collection, dashboards) exists or will be deployed alongside pipeline
- Standard industry retention policies apply unless specified (logs retained 30 days, metrics 90 days)
- Pipeline will be deployed in environment with reasonable resources (4+ CPU cores, 8+ GB RAM minimum)
