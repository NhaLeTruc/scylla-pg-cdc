---

description: "Task list for ScyllaDB to Postgres CDC Pipeline implementation"
---

# Tasks: ScyllaDB to Postgres CDC Pipeline

**Input**: Design documents from `/specs/001-scylla-pg-cdc/`
**Prerequisites**: plan.md (required), spec.md (required for user stories), research.md, data-model.md, contracts/

**Tests**: Per FR-030 hybrid testing approach - unit tests for custom Python code written before implementation (TDD), integration/e2e tests written after component implementation is complete.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4, US5)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: Files at repository root
- docker/ - Docker Compose and service configurations
- scripts/ - Operational scripts (Bash, Python)
- src/ - Custom Python code (reconciliation, monitoring, utils)
- tests/ - Test suite (unit tests for custom code only per hybrid approach)
- configs/ - Connector and infrastructure configurations
- docs/ - Operational documentation

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [x] T001 Create root directory structure (docker/, scripts/, src/, tests/, configs/, docs/)
- [x] T002 Create Python project files (requirements.txt, pytest.ini, .gitignore, README.md)
- [x] T003 [P] Create .env.example file with environment variable templates per quickstart.md
- [x] T004 [P] Create docker/.dockerignore file

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Docker Infrastructure

- [x] T005 Create docker/docker-compose.yml with Zookeeper service definition
- [x] T006 Add Kafka broker service to docker/docker-compose.yml with exactly-once config
- [x] T007 Add Schema Registry service to docker/docker-compose.yml
- [x] T008 Create docker/kafka-connect/Dockerfile with Confluent Hub connector installation
- [x] T009 Add Kafka Connect service to docker/docker-compose.yml with distributed mode config
- [x] T010 Add ScyllaDB service to docker/docker-compose.yml with CDC enabled
- [x] T011 Add PostgreSQL service to docker/docker-compose.yml
- [x] T012 Add Vault service to docker/docker-compose.yml in dev mode
- [x] T013 [P] Add Prometheus service to docker/docker-compose.yml with monitoring profile
- [x] T014 [P] Add Grafana service to docker/docker-compose.yml with monitoring profile
- [x] T015 [P] Add Jaeger service to docker/docker-compose.yml with monitoring profile
- [x] T016 Create docker/docker-compose.test.yml with test-specific overrides

### Database Initialization

- [x] T017 Create docker/scylla/init.cql with CDC metadata keyspace and example table
- [x] T018 Create docker/postgres/init.sql with cdc_metadata schema and reconciliation tables

### Kafka Connect Configuration

- [x] T019 Create docker/kafka-connect/connectors/scylla-source.json with Scylla CDC connector config
- [x] T020 Create docker/kafka-connect/connectors/postgres-sink.json with JDBC sink config
- [x] T021 Create scripts/deploy-connectors.sh for deploying connectors to Kafka Connect
- [x] T022 Create scripts/monitor-connectors.sh for monitoring connector status

### Vault Setup

- [x] T023 Create docker/vault/policies/ with CDC access policies
- [x] T024 Create docker/vault/init-vault.sh to bootstrap secrets and policies

### Monitoring Configuration

- [x] T025 [P] Create docker/prometheus/prometheus.yml with CDC component scrape configs
- [x] T026 [P] Create docker/grafana/provisioning/ with datasources and dashboards
- [x] T027 [P] Create docker/grafana/dashboards/cdc-overview.json with pipeline dashboard

### Python Utilities Foundation

- [x] T028 Create src/utils/vault_client.py with Vault KV v2 read operations
- [x] T029 Create src/utils/correlation.py with UUID generation and context management
- [x] T030 Create src/utils/schema_validator.py with Avro compatibility checking
- [x] T031 [P] Create src/utils/metrics_collector.py with Prometheus metrics collection

### Unit Tests for Python Utilities (TDD)

- [x] T032 Create tests/unit/test_vault_client.py with comprehensive mock tests
- [x] T033 [P] Create tests/unit/test_correlation.py with UUID and context tests
- [x] T034 [P] Create tests/unit/test_schema_validator.py with compatibility tests

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 5 - Local Development Environment (Priority: P1) 🎯 MVP Prerequisite

**Goal**: Enable developers to run full CDC pipeline locally with Docker Compose

**Independent Test**: Run `docker-compose up`, verify all services healthy, execute sample data replication test

### Implementation for User Story 5

- [ ] T035 [US5] Create scripts/setup-local.sh to initialize Docker environment and verify prerequisites
- [ ] T036 [US5] Create scripts/health-check.sh to verify all service health endpoints
- [ ] T037 [US5] Add test data fixtures in docker/scylla/init.cql for sample ecommerce.users table
- [ ] T038 [US5] Add expected results in docker/postgres/init.sql for validation queries
- [ ] T039 [US5] Update README.md with quick start instructions from quickstart.md
- [ ] T040 [US5] Create scripts/teardown-local.sh to clean up Docker environment

**Checkpoint**: At this point, local development environment is fully functional. Developers can start/stop pipeline locally.

---

## Phase 4: User Story 1 - Basic Change Capture & Replication (Priority: P1) 🎯 MVP

**Goal**: Implement end-to-end data replication from ScyllaDB to Postgres

**Independent Test**: Insert row in ScyllaDB, verify it appears in Postgres within 5 seconds

### Connector Deployment Scripts

- [ ] T041 [US1] Create scripts/deploy-connector.sh with Vault credential injection and Kafka Connect REST API calls
- [ ] T042 [US1] Add connector status checking logic to scripts/health-check.sh
- [ ] T043 [US1] Create scripts/pause-connector.sh for graceful connector pause
- [ ] T044 [US1] Create scripts/resume-connector.sh for connector resume
- [ ] T045 [US1] Create scripts/restart-connector.sh for connector restart

### Connector Configuration

- [ ] T046 [P] [US1] Finalize configs/connectors/scylla-source.json with tasks.max=4 and DLQ config
- [ ] T047 [P] [US1] Finalize configs/connectors/postgres-sink.json with batch.size=3000 and retry config

### Verification Scripts

- [ ] T048 [US1] Create scripts/test-replication.sh to insert test data and verify Postgres replication
- [ ] T049 [US1] Create scripts/measure-latency.sh to measure end-to-end latency with timestamps

**Checkpoint**: At this point, User Story 1 should be fully functional. Basic CDC replication works end-to-end.

---

## Phase 5: User Story 2 - Schema Evolution Handling (Priority: P2)

**Goal**: Handle schema changes automatically without pipeline downtime

**Independent Test**: Add column to ScyllaDB table, insert data with new column, verify Postgres receives new schema

### Schema Management Utilities

- [ ] T050 [US2] Enhance src/utils/schema_validator.py with Schema Registry integration for version comparison
- [ ] T051 [US2] Create scripts/check-schema-compatibility.sh to validate schema changes before applying
- [ ] T052 [US2] Add schema change logging to configs/connect/connect-distributed.properties

### Schema Evolution Testing Scripts

- [ ] T053 [US2] Create scripts/test-schema-add-column.sh to test backward-compatible column addition
- [ ] T054 [US2] Create scripts/test-schema-remove-column.sh to test column removal handling
- [ ] T055 [US2] Create scripts/test-schema-incompatible.sh to verify DLQ routing for incompatible changes

### Unit Tests for Schema Validator (TDD - Write Before Enhanced Implementation)

- [ ] T056 [US2] Add Schema Registry mock integration tests to tests/unit/test_schema_validator.py

**Checkpoint**: User Stories 1 AND 2 should both work independently. Schema evolution is handled gracefully.

---

## Phase 6: User Story 3 - Failure Recovery & Reconciliation (Priority: P2)

**Goal**: Recover from failures without data loss and reconcile inconsistencies

**Independent Test**: Stop pipeline mid-processing, make changes, restart, verify all changes eventually replicated

### Reconciliation Implementation (TDD - Unit Tests First)

- [ ] T057 [US3] Create tests/unit/test_comparer.py with test cases for row comparison logic (WRITE FIRST)
- [ ] T058 [US3] Create tests/unit/test_differ.py with test cases for discrepancy detection algorithms (WRITE FIRST)
- [ ] T059 [US3] Create src/reconciliation/__init__.py
- [ ] T060 [US3] Implement src/reconciliation/comparer.py with ScyllaDB and Postgres row comparison logic
- [ ] T061 [US3] Implement src/reconciliation/differ.py with missing/mismatch/extra detection algorithms
- [ ] T062 [US3] Implement src/reconciliation/repairer.py with INSERT/UPDATE/DELETE repair SQL generation

### Reconciliation Scripts

- [ ] T063 [US3] Create scripts/reconcile.py main CLI with argparse for modes (full, incremental, verify-only)
- [ ] T064 [US3] Add checkpoint management to scripts/reconcile.py for resumable reconciliation
- [ ] T065 [US3] Add batch processing to scripts/reconcile.py for memory efficiency (10k rows per batch)
- [ ] T066 [US3] Add repair logic to scripts/reconcile.py with dry-run support
- [ ] T067 [US3] Create scripts/schedule-reconciliation.sh as cron wrapper with logging
- [ ] T068 [US3] Add reconciliation status command to scripts/reconcile.py (status subcommand)
- [ ] T069 [US3] Add reconciliation report command to scripts/reconcile.py (report subcommand)

### Failure Recovery Testing

- [ ] T070 [US3] Create scripts/test-failure-recovery.sh to simulate connector crash and verify recovery
- [ ] T071 [US3] Create scripts/test-network-partition.sh using Docker network disconnect
- [ ] T072 [US3] Create scripts/test-poison-message.sh to verify DLQ routing

### Dead Letter Queue Monitoring

- [ ] T073 [US3] Create scripts/check-dlq.sh to monitor dead letter queue message counts
- [ ] T074 [US3] Create scripts/replay-dlq.sh to reprocess DLQ messages after fixes

**Checkpoint**: User Stories 1, 2, AND 3 should all work independently. Pipeline recovers from failures gracefully.

---

## Phase 7: User Story 4 - Observability & Monitoring (Priority: P3)

**Goal**: Comprehensive monitoring, logging, and alerting for proactive issue detection

**Independent Test**: Run pipeline, generate events/errors, verify dashboards show accurate metrics and logs have correlation IDs

### Custom Metrics Implementation

- [ ] T075 [P] [US4] Create src/monitoring/__init__.py
- [ ] T076 [P] [US4] Create src/monitoring/metrics.py with Prometheus client for custom reconciliation metrics
- [ ] T077 [P] [US4] Create src/monitoring/alerts.py with Prometheus AlertManager rule definitions

### Monitoring Dashboards

- [ ] T078 [US4] Create docker/grafana/dashboards/connector-health.json for per-connector/task status
- [ ] T079 [US4] Create docker/grafana/dashboards/schema-evolution.json for schema version tracking
- [ ] T080 [US4] Create docker/grafana/dashboards/reconciliation.json for discrepancy trends

### Logging Enhancements

- [ ] T081 [US4] Add structured JSON logging to scripts/reconcile.py with correlation ID propagation
- [ ] T082 [US4] Add correlation ID extraction from Kafka headers to configs/connectors/scylla-source.json
- [ ] T083 [US4] Configure log rotation in docker/docker-compose.yml for all services

### Alerting Rules

- [ ] T084 [US4] Create docker/prometheus/alert-rules.yml with lag, error rate, and DLQ alerts
- [ ] T085 [US4] Add AlertManager service to docker/docker-compose.yml (optional monitoring profile)

### Observability Testing

- [ ] T086 [US4] Create scripts/test-tracing.sh to generate trace and verify end-to-end visibility in Jaeger
- [ ] T087 [US4] Create scripts/generate-load.sh for load testing and metrics validation

**Checkpoint**: Full observability stack operational. All metrics, logs, and traces accessible through dashboards.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T088 [P] Create docs/architecture.md documenting system architecture with diagrams
- [ ] T089 [P] Create docs/runbook.md with operational procedures (deployment, scaling, troubleshooting)
- [ ] T090 [P] Create docs/troubleshooting.md with common issues and solutions from quickstart.md
- [ ] T091 [P] Create docs/scaling.md with capacity planning and horizontal scaling guide
- [ ] T092 [P] Add Python type hints to all src/ modules
- [ ] T093 [P] Add docstrings to all public functions in src/ modules
- [ ] T094 Create scripts/backup-offsets.sh for Kafka Connect offset backup
- [ ] T095 Create scripts/chaos-test.sh for failure injection testing (Toxiproxy integration)
- [ ] T096 Add security hardening to configs/ files (TLS certificates, non-root users)
- [ ] T097 Create performance benchmarking script scripts/benchmark.sh
- [ ] T098 Update README.md with production deployment checklist from quickstart.md

---

## Phase 9: Integration Tests (Post-Implementation)

**Purpose**: End-to-end integration tests written AFTER main codebase is implemented per FR-030

**⚠️ NOTE**: These tests are written AFTER all implementation phases above are complete

- [ ] T099 Create tests/integration/test_end_to_end.py with full pipeline flow test (ScyllaDB → Postgres)
- [ ] T100 [P] Create tests/integration/test_schema_evolution.py with add/remove column scenarios
- [ ] T101 [P] Create tests/integration/test_failure_recovery.py with connector restart scenarios
- [ ] T102 [P] Create tests/integration/fixtures/sample_data.sql with test data for ScyllaDB
- [ ] T103 [P] Create tests/integration/fixtures/expected_results.sql with expected Postgres state
- [ ] T104 Create tests/contract/test_scylla_connector.py validating connector output against contracts/
- [ ] T105 [P] Create tests/contract/test_postgres_sink.py validating connector input against contracts/
- [ ] T106 Configure pytest-docker-compose in pytest.ini for integration test environment
- [ ] T107 Add integration test execution to scripts/test-all.sh script

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 5 - Local Environment (Phase 3)**: Depends on Foundational - MVP prerequisite
- **User Story 1 - Basic Replication (Phase 4)**: Depends on Foundational + US5 - Core MVP
- **User Story 2 - Schema Evolution (Phase 5)**: Depends on Foundational + US5 - Can start after US1 or in parallel
- **User Story 3 - Failure Recovery (Phase 6)**: Depends on Foundational + US5 - Can start after US1 or in parallel
- **User Story 4 - Observability (Phase 7)**: Depends on Foundational + US5 - Can start after US1 or in parallel
- **Polish (Phase 8)**: Depends on all desired user stories being complete
- **Integration Tests (Phase 9)**: Depends on Phase 8 completion per FR-030

### User Story Dependencies

- **User Story 5 (P1)**: Can start after Foundational (Phase 2) - MVP prerequisite for local testing
- **User Story 1 (P1)**: Can start after Foundational + US5 - Core MVP, no dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational + US5 - Independent of US1, but builds on connector framework
- **User Story 3 (P2)**: Can start after Foundational + US5 - Independent of US1/US2, requires reconciliation infrastructure
- **User Story 4 (P3)**: Can start after Foundational + US5 - Independent of other stories, adds observability layer

### Within Each User Story

- **US5 (Local Environment)**:
  - Setup scripts → health checks → documentation
  - All tasks can run in sequence

- **US1 (Basic Replication)**:
  - Connector deployment scripts first
  - Connector configurations in parallel
  - Verification scripts last

- **US2 (Schema Evolution)**:
  - Schema validator utilities first
  - Testing scripts in parallel
  - Unit tests before enhanced implementation

- **US3 (Failure Recovery)**:
  - Unit tests FIRST (TDD)
  - Reconciliation implementation after tests pass
  - Reconciliation scripts after implementation
  - Failure testing scripts last

- **US4 (Observability)**:
  - Custom metrics and logging in parallel
  - Dashboards after metrics available
  - Alerting rules after dashboards
  - Testing scripts last

### Parallel Opportunities

- **Setup (Phase 1)**: T003, T004 can run in parallel
- **Foundational (Phase 2)**:
  - T013, T014, T015 (monitoring services) can run in parallel
  - T025, T026, T027 (monitoring configs) can run in parallel
  - T032, T033, T034 (unit tests) can run in parallel
  - T046, T047 (connector configs) can run in parallel
- **US1**: T046, T047 can run in parallel
- **US2**: T056 runs independently
- **US3**: T057, T058 can run in parallel (different test files)
- **US4**: T075, T076, T077 can run in parallel (different modules)
- **Polish (Phase 8)**: T088, T089, T090, T091, T092, T093 can all run in parallel
- **Integration Tests (Phase 9)**: T100, T101, T102, T103, T105 can run in parallel after T099

### Critical Path

1. Setup (Phase 1)
2. Foundational (Phase 2) - BLOCKING
3. User Story 5 (Phase 3) - MVP prerequisite
4. User Story 1 (Phase 4) - Core MVP
5. Polish (Phase 8) - After US1 at minimum for MVP
6. Integration Tests (Phase 9) - After all implementation

**Minimum MVP**: Phases 1, 2, 3, 4, 8 (partial), 9 (partial)
**Full Feature**: All phases 1-9

---

## Parallel Example: User Story 1 (Basic Replication)

```bash
# Launch connector configs in parallel:
Task T046: "Finalize configs/connectors/scylla-source.json"
Task T047: "Finalize configs/connectors/postgres-sink.json"

# After configs ready, deployment scripts run sequentially:
Task T041: "Create scripts/deploy-connector.sh"
Task T042: "Add connector status checking to scripts/health-check.sh"
...
```

---

## Implementation Strategy

### MVP First (User Story 5 + User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 5 (Local Environment)
4. Complete Phase 4: User Story 1 (Basic Replication)
5. Complete Phase 8: Partial Polish (docs/architecture.md, docs/runbook.md)
6. Complete Phase 9: Partial Integration Tests (test_end_to_end.py only)
7. **STOP and VALIDATE**: Test User Story 1 independently in local environment
8. Deploy/demo if ready

**MVP Delivers**: Working CDC pipeline with local testing capability, basic INSERT/UPDATE/DELETE replication

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 5 → Local environment functional
3. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
4. Add User Story 2 → Test schema evolution → Deploy/Demo
5. Add User Story 3 → Test failure recovery → Deploy/Demo
6. Add User Story 4 → Observability enabled → Deploy/Demo
7. Polish + Integration Tests → Production ready
8. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 5 (prerequisite for all)
3. Once US5 done:
   - Developer A: User Story 1 (core MVP)
   - Developer B: User Story 2 (schema evolution)
   - Developer C: User Story 3 (reconciliation)
   - Developer D: User Story 4 (observability)
4. Stories complete and integrate independently
5. Team completes Polish phase together
6. Team completes Integration Tests together

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Unit tests for custom Python code written BEFORE implementation (TDD per FR-030)
- Integration/e2e tests written AFTER all implementation complete (per FR-030 "DO NOT WRITE ANY TEST until main codebase completely implemented")
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Avoid: vague tasks, same file conflicts, cross-story dependencies that break independence

---

## Task Summary

**Total Tasks**: 107
**By Phase**:
- Setup: 4 tasks
- Foundational: 30 tasks
- User Story 5 (Local Environment): 6 tasks
- User Story 1 (Basic Replication): 9 tasks
- User Story 2 (Schema Evolution): 7 tasks
- User Story 3 (Failure Recovery): 18 tasks
- User Story 4 (Observability): 13 tasks
- Polish: 11 tasks
- Integration Tests: 9 tasks

**Parallel Opportunities**: 24 tasks marked [P] can run in parallel with other tasks in same phase

**MVP Scope** (Minimum viable product):
- Phases 1, 2, 3, 4 + partial Phase 8 + partial Phase 9 = ~55 tasks
- Est. effort: 3-5 developer-weeks

**Full Feature Scope**:
- All 107 tasks
- Est. effort: 6-10 developer-weeks
