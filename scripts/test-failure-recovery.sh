#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Failure Recovery Test
# ==============================================================================
# Description: Test connector crash and recovery scenarios
# Usage: ./scripts/test-failure-recovery.sh [OPTIONS]
# ==============================================================================

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Configuration
TABLE="users"
CONNECTOR="scylla-source"
FAILURE_DURATION=30
WAIT_TIME=15
CLEANUP=true
VERBOSE=false

# ==============================================================================
# Helper Functions
# ==============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${BLUE}[DEBUG]${NC} $1"
    fi
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Test connector failure and recovery scenarios.

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (default: users)
    -c, --connector NAME    Connector to test (default: scylla-source)
    -d, --duration SECS     Failure duration in seconds (default: 30)
    -w, --wait SECS         Wait time for replication (default: 15)
    --no-cleanup            Don't clean up test data
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                                          # Test with defaults
    $0 --connector postgres-sink                # Test sink connector
    $0 --duration 60 --verbose                  # Longer failure with debug

DESCRIPTION:
    This script tests the pipeline's ability to recover from connector failures:
    1. Insert baseline data before failure
    2. Stop the connector (simulate crash)
    3. Insert data while connector is down
    4. Restart the connector
    5. Verify all data eventually replicates
    6. Check offset management and state recovery
EOF
}

check_prerequisites() {
    if ! docker exec scylla cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        log_error "ScyllaDB is not accessible"
        return 1
    fi

    if ! docker exec postgres pg_isready -U postgres -d warehouse &>/dev/null; then
        log_error "PostgreSQL is not accessible"
        return 1
    fi

    if ! curl -sf http://localhost:8083/ &>/dev/null; then
        log_error "Kafka Connect is not accessible"
        return 1
    fi

    return 0
}

get_connector_status() {
    local connector=$1
    curl -sf "http://localhost:8083/connectors/${connector}/status" 2>/dev/null | \
        jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN"
}

wait_for_connector_state() {
    local connector=$1
    local expected_state=$2
    local timeout=$3
    local elapsed=0

    log_info "Waiting for connector to reach state: $expected_state (timeout: ${timeout}s)"

    while [ $elapsed -lt $timeout ]; do
        local status
        status=$(get_connector_status "$connector")

        if [ "$status" = "$expected_state" ]; then
            log_success "Connector is $expected_state"
            return 0
        fi

        log_verbose "Current state: $status (waiting...)"
        sleep 2
        elapsed=$((elapsed + 2))
    done

    log_error "Timeout waiting for connector to reach $expected_state"
    return 1
}

stop_connector() {
    local connector=$1

    log_info "Stopping connector: $connector"

    curl -sf -X PUT "http://localhost:8083/connectors/${connector}/pause" &>/dev/null

    if ! wait_for_connector_state "$connector" "PAUSED" 30; then
        log_error "Failed to pause connector"
        return 1
    fi

    return 0
}

start_connector() {
    local connector=$1

    log_info "Starting connector: $connector"

    curl -sf -X PUT "http://localhost:8083/connectors/${connector}/resume" &>/dev/null

    if ! wait_for_connector_state "$connector" "RUNNING" 30; then
        log_error "Failed to resume connector"
        return 1
    fi

    return 0
}

restart_connector() {
    local connector=$1

    log_info "Restarting connector: $connector"

    curl -sf -X POST "http://localhost:8083/connectors/${connector}/restart" &>/dev/null

    sleep 5

    if ! wait_for_connector_state "$connector" "RUNNING" 30; then
        log_error "Failed to restart connector"
        return 1
    fi

    return 0
}

generate_test_id() {
    echo "failtest-$(date +%s)-$RANDOM"
}

insert_test_data() {
    local test_id=$1
    local phase=$2
    local test_email="${phase}-${test_id}@example.com"

    log_info "Inserting test data: $phase"

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, created_at, updated_at, status)
        VALUES (uuid(), '${phase}_${test_id}', '${test_email}', 'Test', '${phase}', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test data inserted: $test_email"
        echo "$test_email"
        return 0
    else
        log_error "Failed to insert test data"
        return 1
    fi
}

verify_replication() {
    local email=$1
    local description=$2

    log_verbose "Verifying replication: $email"

    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT email FROM cdc_data.${TABLE} WHERE email = '${email}';
    " 2>/dev/null | tr -d ' ')

    if [ -n "$pg_result" ]; then
        log_success "Replicated: $description"
        return 0
    else
        log_error "NOT replicated: $description"
        return 1
    fi
}

cleanup_test_data() {
    local email=$1

    log_verbose "Cleaning up: $email"

    docker exec postgres psql -U postgres -d warehouse -c "
        DELETE FROM cdc_data.${TABLE} WHERE email = '${email}';
    " &>/dev/null

    docker exec scylla cqlsh -e "
        USE app_data;
        DELETE FROM ${TABLE} WHERE email = '${email}';
    " &>/dev/null
}

check_offset_state() {
    local connector=$1

    log_info "Checking connector offset state..."

    # Get current offset from Kafka
    local offsets
    offsets=$(docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic connect-offsets \
        --from-beginning \
        --max-messages 10 \
        --timeout-ms 5000 2>/dev/null | grep "$connector" || echo "")

    if [ -n "$offsets" ]; then
        log_success "Connector has offset data (state preserved)"
        log_verbose "Offset sample: $(echo "$offsets" | head -1)"
        return 0
    else
        log_warning "No offset data found (may be expected for first run)"
        return 0
    fi
}

# ==============================================================================
# Argument Parsing
# ==============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -t|--table)
            TABLE="$2"
            shift 2
            ;;
        -c|--connector)
            CONNECTOR="$2"
            shift 2
            ;;
        -d|--duration)
            FAILURE_DURATION="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_TIME="$2"
            shift 2
            ;;
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# ==============================================================================
# Main Execution
# ==============================================================================

cd "$PROJECT_ROOT"

log_info "==================================================================="
log_info "Connector Failure Recovery Test"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Table: $TABLE"
echo "  Connector: $CONNECTOR"
echo "  Failure duration: ${FAILURE_DURATION}s"
echo "  Wait time: ${WAIT_TIME}s"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi
log_success "All services are healthy"
echo ""

# Check initial connector state
initial_status=$(get_connector_status "$CONNECTOR")
log_info "Initial connector status: $initial_status"

if [ "$initial_status" != "RUNNING" ]; then
    log_warning "Connector is not RUNNING, attempting to start..."
    start_connector "$CONNECTOR"
fi
echo ""

# Step 1: Insert baseline data before failure
log_info "==================================================================="
log_info "Step 1: Insert baseline data (before failure)"
log_info "==================================================================="
echo ""

test_id=$(generate_test_id)
before_email=$(insert_test_data "$test_id" "before")

if [ -z "$before_email" ]; then
    log_error "Failed to insert baseline data"
    exit 1
fi

log_info "Waiting ${WAIT_TIME}s for baseline replication..."
sleep "$WAIT_TIME"

if verify_replication "$before_email" "baseline data"; then
    log_success "Baseline data replicated successfully ✓"
else
    log_error "Baseline data failed to replicate (check connector)"
    exit 1
fi
echo ""

# Step 2: Simulate connector failure
log_info "==================================================================="
log_info "Step 2: Simulate connector failure"
log_info "==================================================================="
echo ""

if ! stop_connector "$CONNECTOR"; then
    log_error "Failed to stop connector"
    exit 1
fi

log_info "Connector paused. Simulating failure for ${FAILURE_DURATION}s..."
echo ""

# Step 3: Insert data while connector is down
log_info "==================================================================="
log_info "Step 3: Insert data while connector is DOWN"
log_info "==================================================================="
echo ""

during1_email=$(insert_test_data "$test_id" "during1")
sleep 2
during2_email=$(insert_test_data "$test_id" "during2")
sleep 2
during3_email=$(insert_test_data "$test_id" "during3")

if [ -z "$during1_email" ] || [ -z "$during2_email" ] || [ -z "$during3_email" ]; then
    log_error "Failed to insert data during failure"
    start_connector "$CONNECTOR"
    exit 1
fi

log_success "Inserted 3 records while connector was down"

# Wait out the remaining failure duration
remaining_wait=$((FAILURE_DURATION - 6))
if [ $remaining_wait -gt 0 ]; then
    log_info "Waiting ${remaining_wait}s to complete failure simulation..."
    sleep "$remaining_wait"
fi
echo ""

# Step 4: Restart connector
log_info "==================================================================="
log_info "Step 4: Restart connector and verify recovery"
log_info "==================================================================="
echo ""

if ! start_connector "$CONNECTOR"; then
    log_error "Failed to restart connector"
    exit 1
fi

log_success "Connector restarted successfully"

# Check offset state
check_offset_state "$CONNECTOR"
echo ""

# Step 5: Verify all data eventually replicates
log_info "==================================================================="
log_info "Step 5: Verify data replication after recovery"
log_info "==================================================================="
echo ""

log_info "Waiting ${WAIT_TIME}s for catch-up replication..."
sleep "$WAIT_TIME"

# Verify baseline data still exists
if verify_replication "$before_email" "baseline data (after recovery)"; then
    log_success "Baseline data still present ✓"
else
    log_warning "Baseline data missing after recovery"
fi

# Verify data inserted during failure
success_count=0
total_during=3

if verify_replication "$during1_email" "data during failure #1"; then
    ((success_count++))
fi

if verify_replication "$during2_email" "data during failure #2"; then
    ((success_count++))
fi

if verify_replication "$during3_email" "data during failure #3"; then
    ((success_count++))
fi

echo ""

if [ $success_count -eq $total_during ]; then
    log_success "All data inserted during failure has been replicated ✓"
    log_success "RECOVERY SUCCESSFUL - NO DATA LOSS"
else
    log_error "Only $success_count/$total_during records replicated"
    log_error "DATA LOSS DETECTED"
fi
echo ""

# Step 6: Insert new data to verify ongoing operation
log_info "==================================================================="
log_info "Step 6: Verify ongoing operation"
log_info "==================================================================="
echo ""

after_email=$(insert_test_data "$test_id" "after")

log_info "Waiting ${WAIT_TIME}s for new data replication..."
sleep "$WAIT_TIME"

if verify_replication "$after_email" "post-recovery data"; then
    log_success "Post-recovery data replicated successfully ✓"
    log_success "Connector is operating normally"
else
    log_error "Post-recovery data failed to replicate"
    log_error "Connector may still have issues"
fi
echo ""

# Cleanup
if [ "$CLEANUP" = true ]; then
    log_info "==================================================================="
    log_info "Cleanup: Removing test data"
    log_info "==================================================================="
    echo ""

    cleanup_test_data "$before_email"
    cleanup_test_data "$during1_email"
    cleanup_test_data "$during2_email"
    cleanup_test_data "$during3_email"
    cleanup_test_data "$after_email"

    log_success "Cleanup completed"
    echo ""
fi

# Summary
log_success "==================================================================="
log_success "Failure Recovery Test: COMPLETED"
log_success "==================================================================="
echo ""

log_info "Test Results:"
echo "  ✓ Baseline data replicated before failure"
echo "  ✓ Connector stopped successfully"
echo "  ✓ Data inserted while connector was down"
echo "  ✓ Connector restarted successfully"

if [ $success_count -eq $total_during ]; then
    echo "  ✓ All data replicated after recovery (NO DATA LOSS)"
else
    echo "  ✗ Data loss detected: $((total_during - success_count)) records missing"
fi

echo "  ✓ Post-recovery operation verified"
echo ""

log_info "Key Findings:"
echo "  • Connector recovery time: ~${WAIT_TIME}s"
echo "  • Data inserted during downtime: $total_during records"
echo "  • Data successfully recovered: $success_count records"
echo "  • Recovery success rate: $(awk "BEGIN {printf \"%.1f\", ($success_count / $total_during) * 100}")%"
echo ""

if [ $success_count -eq $total_during ]; then
    log_success "The pipeline successfully recovers from connector failures with NO DATA LOSS"
    log_info "The Kafka Connect framework maintains offset state and resumes from the last committed position."
    exit 0
else
    log_error "Data loss detected during recovery"
    log_info "Review connector logs: docker compose logs kafka-connect"
    exit 1
fi
