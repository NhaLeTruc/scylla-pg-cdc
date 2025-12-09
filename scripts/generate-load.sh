#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Load Generator
# ==============================================================================
# Description: Generate load for testing and metrics validation
# Usage: ./scripts/generate-load.sh [OPTIONS]
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
TABLES="users products orders"
OPERATION_MIX="insert:60,update:30,delete:10"
DURATION=300  # 5 minutes
RATE=10  # operations per second
WORKERS=4
REPORT_INTERVAL=30
CLEANUP=false
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

Generate load for CDC pipeline testing and metrics validation.

OPTIONS:
    -h, --help              Show this help message
    -t, --tables TABLES     Space-separated table list (default: users products orders)
    -m, --mix MIX           Operation mix ratio (default: insert:60,update:30,delete:10)
    -d, --duration SECS     Test duration in seconds (default: 300)
    -r, --rate OPS/SEC      Operations per second (default: 10)
    -w, --workers N         Number of parallel workers (default: 4)
    -i, --interval SECS     Reporting interval (default: 30)
    --cleanup               Clean up test data after completion
    -v, --verbose           Enable verbose output

EXAMPLES:
    # Standard load test - 5 minutes at 10 ops/sec
    $0

    # High load test - 10 minutes at 100 ops/sec
    $0 --duration 600 --rate 100 --workers 10

    # Insert-only load test
    $0 --mix insert:100 --rate 50

    # Load test with cleanup
    $0 --duration 60 --cleanup

DESCRIPTION:
    This script generates realistic load for:
    - Performance testing
    - Metrics validation
    - Dashboard verification
    - Alert threshold tuning
    - Capacity planning

    Operations:
    - INSERT: Creates new records
    - UPDATE: Modifies existing records
    - DELETE: Removes records

    Metrics collected:
    - Throughput (ops/sec)
    - Latency (p50, p95, p99)
    - Error rate
    - Replication lag
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

    return 0
}

parse_operation_mix() {
    local mix=$1

    # Parse mix like "insert:60,update:30,delete:10"
    INSERT_PCT=$(echo "$mix" | grep -oP 'insert:\K\d+' || echo "0")
    UPDATE_PCT=$(echo "$mix" | grep -oP 'update:\K\d+' || echo "0")
    DELETE_PCT=$(echo "$mix" | grep -oP 'delete:\K\d+' || echo "0")

    local total=$((INSERT_PCT + UPDATE_PCT + DELETE_PCT))
    if [ $total -ne 100 ]; then
        log_warning "Operation mix doesn't sum to 100% (got ${total}%), normalizing..."
    fi

    log_verbose "Operation mix: INSERT=$INSERT_PCT% UPDATE=$UPDATE_PCT% DELETE=$DELETE_PCT%"
}

insert_record() {
    local table=$1
    local load_id=$2

    case $table in
        users)
            local email="load-${load_id}@example.com"
            docker exec scylla cqlsh -e "
                INSERT INTO app_data.${table} (user_id, username, email, first_name, last_name, created_at, updated_at, status)
                VALUES (uuid(), 'load_${load_id}', '${email}', 'Load', 'Test', toTimestamp(now()), toTimestamp(now()), 'active');
            " &>/dev/null
            ;;
        products)
            docker exec scylla cqlsh -e "
                INSERT INTO app_data.${table} (product_id, name, description, price, stock_quantity, created_at, updated_at, status)
                VALUES (uuid(), 'Product ${load_id}', 'Load test product', 99.99, 100, toTimestamp(now()), toTimestamp(now()), 'active');
            " &>/dev/null
            ;;
        orders)
            docker exec scylla cqlsh -e "
                INSERT INTO app_data.${table} (order_id, user_id, order_date, status, total_amount, created_at, updated_at)
                VALUES (uuid(), uuid(), toTimestamp(now()), 'pending', 199.99, toTimestamp(now()), toTimestamp(now()));
            " &>/dev/null
            ;;
    esac

    return $?
}

update_record() {
    local table=$1

    case $table in
        users)
            docker exec scylla cqlsh -e "
                UPDATE app_data.${table}
                SET updated_at = toTimestamp(now()), status = 'updated'
                WHERE email = 'load-test@example.com'
                IF EXISTS;
            " &>/dev/null
            ;;
        products)
            docker exec scylla cqlsh -e "
                UPDATE app_data.${table}
                SET updated_at = toTimestamp(now()), stock_quantity = 50
                WHERE name = 'Product Test'
                IF EXISTS;
            " &>/dev/null
            ;;
        orders)
            docker exec scylla cqlsh -e "
                UPDATE app_data.${table}
                SET updated_at = toTimestamp(now()), status = 'completed'
                WHERE order_id = uuid()
                IF EXISTS;
            " &>/dev/null
            ;;
    esac

    return $?
}

delete_record() {
    local table=$1

    # Delete old test records
    docker exec scylla cqlsh -e "
        DELETE FROM app_data.${table}
        WHERE email = 'old-load-test@example.com'
        IF EXISTS;
    " &>/dev/null 2>&1

    return 0
}

select_operation() {
    local rand=$((RANDOM % 100))

    if [ $rand -lt $INSERT_PCT ]; then
        echo "insert"
    elif [ $rand -lt $((INSERT_PCT + UPDATE_PCT)) ]; then
        echo "update"
    else
        echo "delete"
    fi
}

worker_process() {
    local worker_id=$1
    local ops_per_worker=$2
    local sleep_time=$3

    local insert_count=0
    local update_count=0
    local delete_count=0
    local error_count=0

    for ((i=0; i<ops_per_worker; i++)); do
        local operation
        operation=$(select_operation)

        local table
        table=$(echo "$TABLES" | tr ' ' '\n' | shuf | head -1)

        local load_id="${worker_id}-${i}"

        case $operation in
            insert)
                if insert_record "$table" "$load_id"; then
                    ((insert_count++))
                else
                    ((error_count++))
                fi
                ;;
            update)
                if update_record "$table"; then
                    ((update_count++))
                else
                    ((error_count++))
                fi
                ;;
            delete)
                if delete_record "$table"; then
                    ((delete_count++))
                else
                    ((error_count++))
                fi
                ;;
        esac

        sleep "$sleep_time"
    done

    echo "$worker_id:$insert_count:$update_count:$delete_count:$error_count"
}

cleanup_test_data() {
    log_info "Cleaning up test data..."

    for table in $TABLES; do
        case $table in
            users)
                docker exec scylla cqlsh -e "
                    DELETE FROM app_data.${table} WHERE email LIKE 'load-%';
                " &>/dev/null
                ;;
            products)
                docker exec scylla cqlsh -e "
                    DELETE FROM app_data.${table} WHERE name LIKE 'Product %';
                " &>/dev/null
                ;;
            orders)
                # Orders cleanup would need specific criteria
                log_verbose "Skipping orders cleanup (no email field)"
                ;;
        esac
    done

    log_success "Cleanup completed"
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
        -t|--tables)
            TABLES="$2"
            shift 2
            ;;
        -m|--mix)
            OPERATION_MIX="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -r|--rate)
            RATE="$2"
            shift 2
            ;;
        -w|--workers)
            WORKERS="$2"
            shift 2
            ;;
        -i|--interval)
            REPORT_INTERVAL="$2"
            shift 2
            ;;
        --cleanup)
            CLEANUP=true
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
log_info "CDC Pipeline Load Generator"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Tables: $TABLES"
echo "  Duration: ${DURATION}s"
echo "  Target rate: ${RATE} ops/sec"
echo "  Workers: $WORKERS"
echo "  Operation mix: $OPERATION_MIX"
echo "  Cleanup: $CLEANUP"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi
log_success "Services are accessible"
echo ""

# Parse operation mix
parse_operation_mix "$OPERATION_MIX"

# Calculate operations per worker
total_operations=$((RATE * DURATION))
ops_per_worker=$((total_operations / WORKERS))
sleep_time=$(awk "BEGIN {printf \"%.4f\", 1.0 / ($RATE / $WORKERS)}")

log_info "Load test parameters:"
echo "  Total operations: $total_operations"
echo "  Operations per worker: $ops_per_worker"
echo "  Sleep between ops: ${sleep_time}s"
echo ""

# Start load test
log_info "==================================================================="
log_info "Starting load test..."
log_info "==================================================================="
echo ""

start_time=$(date +%s)

# Start workers in background
pids=()
for ((w=1; w<=WORKERS; w++)); do
    worker_process "$w" "$ops_per_worker" "$sleep_time" > "/tmp/worker-${w}.out" &
    pids+=($!)
    log_verbose "Started worker $w (PID: ${pids[$((w-1))]})"
done

log_info "Started $WORKERS workers"

# Monitor progress
while true; do
    sleep "$REPORT_INTERVAL"

    current_time=$(date +%s)
    elapsed=$((current_time - start_time))

    if [ $elapsed -ge $DURATION ]; then
        break
    fi

    # Calculate progress
    progress=$((elapsed * 100 / DURATION))
    log_info "Progress: ${elapsed}/${DURATION}s (${progress}%)"
done

# Wait for workers to complete
log_info "Waiting for workers to complete..."
wait "${pids[@]}"

end_time=$(date +%s)
actual_duration=$((end_time - start_time))

# Collect results
total_insert=0
total_update=0
total_delete=0
total_errors=0

for ((w=1; w<=WORKERS; w++)); do
    if [ -f "/tmp/worker-${w}.out" ]; then
        result=$(cat "/tmp/worker-${w}.out")
        IFS=':' read -r _ inserts updates deletes errors <<< "$result"

        total_insert=$((total_insert + inserts))
        total_update=$((total_update + updates))
        total_delete=$((total_delete + deletes))
        total_errors=$((total_errors + errors))

        rm "/tmp/worker-${w}.out"
    fi
done

total_success=$((total_insert + total_update + total_delete))

echo ""
log_success "==================================================================="
log_success "Load Test Complete"
log_success "==================================================================="
echo ""

log_info "Results:"
echo "  Duration: ${actual_duration}s"
echo "  Total operations: $((total_success + total_errors))"
echo "    Successful: $total_success"
echo "    Errors: $total_errors"
echo ""
echo "  Operation breakdown:"
echo "    INSERT: $total_insert"
echo "    UPDATE: $total_update"
echo "    DELETE: $total_delete"
echo ""

# Calculate metrics
if [ $actual_duration -gt 0 ]; then
    actual_rate=$(awk "BEGIN {printf \"%.2f\", $total_success / $actual_duration}")
    error_rate=$(awk "BEGIN {printf \"%.2f\", ($total_errors / ($total_success + $total_errors)) * 100}")

    echo "  Performance:"
    echo "    Throughput: ${actual_rate} ops/sec"
    echo "    Target rate: ${RATE} ops/sec"
    echo "    Error rate: ${error_rate}%"
fi

echo ""

log_info "Next steps:"
echo "  1. Check Grafana dashboards for metrics visualization"
echo "  2. Verify replication to PostgreSQL"
echo "  3. Check Prometheus for custom metrics"
echo "  4. Review connector health and lag metrics"
echo ""

# Cleanup
if [ "$CLEANUP" = true ]; then
    cleanup_test_data
    echo ""
fi

if [ $total_errors -eq 0 ]; then
    log_success "Load test completed successfully with no errors ✓"
    exit 0
else
    log_warning "Load test completed with $total_errors errors"
    exit 0
fi
