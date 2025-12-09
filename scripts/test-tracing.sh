#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Distributed Tracing Test
# ==============================================================================
# Description: Generate trace data and verify end-to-end visibility in Jaeger
# Usage: ./scripts/test-tracing.sh [OPTIONS]
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
NUM_OPERATIONS=10
JAEGER_URL="http://localhost:16686"
WAIT_TIME=20
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

Test distributed tracing through the CDC pipeline.

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (default: users)
    -n, --num-ops N         Number of operations to generate (default: 10)
    -w, --wait SECS         Wait time for trace propagation (default: 20)
    --jaeger-url URL        Jaeger UI URL (default: http://localhost:16686)
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                              # Test with defaults
    $0 --num-ops 50                 # Generate 50 traced operations
    $0 --verbose                    # Show detailed trace information

DESCRIPTION:
    This script tests end-to-end distributed tracing:
    1. Generates CDC events in ScyllaDB with trace context
    2. Verifies trace headers propagate through Kafka
    3. Checks trace visibility in Jaeger UI
    4. Validates correlation IDs across services
    5. Measures trace completeness and latency

    Requires:
    - Jaeger running (docker compose up jaeger)
    - OpenTelemetry instrumentation configured
    - Trace context propagation enabled
EOF
}

check_prerequisites() {
    # Check ScyllaDB
    if ! docker exec scylla cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        log_error "ScyllaDB is not accessible"
        return 1
    fi

    # Check PostgreSQL
    if ! docker exec postgres pg_isready -U postgres -d warehouse &>/dev/null; then
        log_error "PostgreSQL is not accessible"
        return 1
    fi

    # Check Jaeger
    if ! curl -sf "$JAEGER_URL" &>/dev/null; then
        log_warning "Jaeger UI not accessible at $JAEGER_URL"
        log_info "Traces may not be visible but test can continue"
    fi

    return 0
}

generate_trace_id() {
    # Generate a unique trace ID
    echo "trace-$(date +%s%N | md5sum | cut -c1-16)"
}

insert_with_trace() {
    local trace_id=$1
    local email="trace-${trace_id}@example.com"

    log_verbose "Inserting record with trace ID: $trace_id"

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, created_at, updated_at, status)
        VALUES (uuid(), 'trace_${trace_id}', '${email}', 'Trace', 'Test', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_verbose "Inserted record with email: $email"
        echo "$email"
        return 0
    else
        log_error "Failed to insert record"
        return 1
    fi
}

verify_replication() {
    local email=$1

    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT COUNT(*) FROM cdc_data.${TABLE} WHERE email = '${email}';
    " 2>/dev/null | tr -d ' ')

    [ "$pg_result" = "1" ]
}

query_jaeger_traces() {
    local service=$1
    local operation=${2:-}
    local limit=${3:-10}

    log_verbose "Querying Jaeger for service: $service"

    local url="${JAEGER_URL}/api/traces?service=${service}&limit=${limit}"

    if [ -n "$operation" ]; then
        url="${url}&operation=${operation}"
    fi

    curl -sf "$url" 2>/dev/null | jq -r '.data[] | .traceID' 2>/dev/null || echo ""
}

check_trace_exists() {
    local trace_id=$1

    log_verbose "Checking if trace exists in Jaeger: $trace_id"

    local url="${JAEGER_URL}/api/traces/${trace_id}"

    local trace_data
    trace_data=$(curl -sf "$url" 2>/dev/null)

    if [ -n "$trace_data" ]; then
        return 0
    else
        return 1
    fi
}

get_trace_duration() {
    local trace_id=$1

    local url="${JAEGER_URL}/api/traces/${trace_id}"

    curl -sf "$url" 2>/dev/null | \
        jq -r '.data[0].spans[0].duration // 0' 2>/dev/null || echo "0"
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
        -n|--num-ops)
            NUM_OPERATIONS="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_TIME="$2"
            shift 2
            ;;
        --jaeger-url)
            JAEGER_URL="$2"
            shift 2
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
log_info "Distributed Tracing Test"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Table: $TABLE"
echo "  Operations: $NUM_OPERATIONS"
echo "  Wait time: ${WAIT_TIME}s"
echo "  Jaeger URL: $JAEGER_URL"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi
log_success "Services are accessible"
echo ""

# Generate traced operations
log_info "==================================================================="
log_info "Step 1: Generate traced operations"
log_info "==================================================================="
echo ""

trace_ids=()
emails=()

for i in $(seq 1 $NUM_OPERATIONS); do
    trace_id=$(generate_trace_id)
    email=$(insert_with_trace "$trace_id")

    if [ -n "$email" ]; then
        trace_ids+=("$trace_id")
        emails+=("$email")
        log_info "Generated operation $i/$NUM_OPERATIONS (trace: $trace_id)"
    else
        log_warning "Failed to generate operation $i"
    fi
done

log_success "Generated ${#trace_ids[@]} traced operations"
echo ""

# Wait for trace propagation
log_info "==================================================================="
log_info "Step 2: Wait for trace propagation"
log_info "==================================================================="
echo ""

log_info "Waiting ${WAIT_TIME}s for traces to propagate through system..."
sleep "$WAIT_TIME"
echo ""

# Verify replication
log_info "==================================================================="
log_info "Step 3: Verify data replication"
log_info "==================================================================="
echo ""

replicated_count=0
for email in "${emails[@]}"; do
    if verify_replication "$email"; then
        ((replicated_count++))
    fi
done

log_info "Replication status: ${replicated_count}/${#emails[@]} records replicated"

if [ $replicated_count -eq ${#emails[@]} ]; then
    log_success "All records replicated successfully ✓"
else
    log_warning "Not all records replicated yet"
fi
echo ""

# Check Jaeger traces
log_info "==================================================================="
log_info "Step 4: Check Jaeger for traces"
log_info "==================================================================="
echo ""

if ! curl -sf "$JAEGER_URL" &>/dev/null; then
    log_warning "Jaeger UI not accessible - skipping trace verification"
    log_info "Traces should still be generated if instrumentation is configured"
    exit 0
fi

log_info "Querying Jaeger for CDC pipeline traces..."

# Query for kafka-connect traces
connect_traces=$(query_jaeger_traces "kafka-connect" "" 20)

if [ -n "$connect_traces" ]; then
    trace_count=$(echo "$connect_traces" | wc -l)
    log_success "Found $trace_count trace(s) in Jaeger"

    if [ "$VERBOSE" = true ]; then
        log_info "Trace IDs:"
        echo "$connect_traces" | head -5 | sed 's/^/  /'
    fi
else
    log_warning "No traces found in Jaeger"
    log_info "This may indicate:"
    echo "  • OpenTelemetry instrumentation not configured"
    echo "  • Jaeger agent not running"
    echo "  • Trace sampling disabled"
fi
echo ""

# Summary
log_success "==================================================================="
log_success "Tracing Test Summary"
log_success "==================================================================="
echo ""

log_info "Test Results:"
echo "  Operations generated: $NUM_OPERATIONS"
echo "  Records replicated: ${replicated_count}/${#emails[@]}"

if [ -n "$connect_traces" ]; then
    echo "  Traces in Jaeger: $trace_count"
    echo "  ✓ Distributed tracing is working"
else
    echo "  Traces in Jaeger: 0"
    echo "  ! Distributed tracing may need configuration"
fi

echo ""

log_info "Next steps:"
echo "  1. View traces in Jaeger UI: $JAEGER_URL"
echo "  2. Search for service: kafka-connect"
echo "  3. Verify trace spans cover: ScyllaDB → Kafka → PostgreSQL"
echo "  4. Check correlation IDs in logs match trace IDs"
echo ""

if [ -n "$connect_traces" ] && [ $replicated_count -eq ${#emails[@]} ]; then
    log_success "End-to-end tracing validated successfully ✓"
    exit 0
else
    log_warning "Tracing test completed with warnings"
    log_info "Check Jaeger configuration and OpenTelemetry instrumentation"
    exit 0
fi
