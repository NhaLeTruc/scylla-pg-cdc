#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Latency Measurement Script
# ==============================================================================
# Description: Measure end-to-end CDC replication latency with timestamps
# Usage: ./scripts/measure-latency.sh [OPTIONS]
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
MEASUREMENTS=10
MAX_WAIT=30
TABLE="users"
INTERVAL=2

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

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Measure end-to-end latency of CDC replication from ScyllaDB to PostgreSQL.

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (users, products, orders) (default: users)
    -n, --measurements N    Number of measurements to take (default: 10)
    -m, --max-wait SECONDS  Maximum wait time per measurement (default: 30)
    -i, --interval SECONDS  Interval between measurements (default: 2)

EXAMPLES:
    $0                                  # Measure users table with defaults
    $0 --measurements 20 --interval 5   # 20 measurements, 5s apart
    $0 --table products --max-wait 60   # Products table, 60s max wait

DESCRIPTION:
    This script measures CDC replication latency by:
    1. Recording timestamp when inserting data in ScyllaDB
    2. Polling PostgreSQL until data appears
    3. Recording timestamp when data is found
    4. Calculating elapsed time (latency)
    5. Computing statistics (min, max, avg, p50, p95, p99)
EOF
}

check_services() {
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

    # Check Kafka Connect
    if ! curl -sf http://localhost:8083/ &>/dev/null; then
        log_error "Kafka Connect is not accessible"
        return 1
    fi

    return 0
}

generate_test_id() {
    echo "latency-$(date +%s%N | cut -b1-13)-$RANDOM"
}

measure_latency_users() {
    local test_id=$1
    local test_email="latency-${test_id}@example.com"
    local start_time
    local end_time
    local elapsed
    local poll_count=0
    local max_polls=$((MAX_WAIT * 10))  # Poll every 100ms

    # Insert data and record start time
    start_time=$(date +%s%3N)  # Milliseconds
    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status)
        VALUES (uuid(), 'latency_${test_id}', '${test_email}', 'Latency', 'Test', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -ne 0 ]; then
        echo "-1"  # Error
        return 1
    fi

    # Poll PostgreSQL until data appears
    while [ $poll_count -lt $max_polls ]; do
        local found
        found=$(docker exec postgres psql -U postgres -d warehouse -t -c "
            SELECT COUNT(*) FROM cdc_data.users WHERE email = '${test_email}';
        " 2>/dev/null | tr -d ' ')

        if [ "$found" = "1" ]; then
            end_time=$(date +%s%3N)
            elapsed=$((end_time - start_time))

            # Cleanup
            docker exec postgres psql -U postgres -d warehouse -c "
                DELETE FROM cdc_data.users WHERE email = '${test_email}';
            " &>/dev/null
            docker exec scylla cqlsh -e "
                USE app_data;
                DELETE FROM users WHERE email = '${test_email}';
            " &>/dev/null

            echo "$elapsed"
            return 0
        fi

        sleep 0.1
        ((poll_count++))
    done

    # Timeout
    echo "-2"  # Timeout
    return 1
}

measure_latency_products() {
    local test_id=$1
    local test_sku="LAT-${test_id}"
    local start_time
    local end_time
    local elapsed
    local poll_count=0
    local max_polls=$((MAX_WAIT * 10))

    start_time=$(date +%s%3N)
    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at)
        VALUES (uuid(), '${test_sku}', 'Latency Test Product', 'Test', 'Test', 1.00, 'USD', 1, true, toTimestamp(now()), toTimestamp(now()));
    " &>/dev/null

    if [ $? -ne 0 ]; then
        echo "-1"
        return 1
    fi

    while [ $poll_count -lt $max_polls ]; do
        local found
        found=$(docker exec postgres psql -U postgres -d warehouse -t -c "
            SELECT COUNT(*) FROM cdc_data.products WHERE sku = '${test_sku}';
        " 2>/dev/null | tr -d ' ')

        if [ "$found" = "1" ]; then
            end_time=$(date +%s%3N)
            elapsed=$((end_time - start_time))

            docker exec postgres psql -U postgres -d warehouse -c "
                DELETE FROM cdc_data.products WHERE sku = '${test_sku}';
            " &>/dev/null
            docker exec scylla cqlsh -e "
                USE app_data;
                DELETE FROM products WHERE sku = '${test_sku}';
            " &>/dev/null

            echo "$elapsed"
            return 0
        fi

        sleep 0.1
        ((poll_count++))
    done

    echo "-2"
    return 1
}

calculate_percentile() {
    local values=("$@")
    local percentile=$1
    shift
    values=("$@")

    local count=${#values[@]}
    local index=$(awk "BEGIN {printf \"%.0f\", ($percentile / 100) * ($count - 1)}")

    echo "${values[$index]}"
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
        -n|--measurements)
            MEASUREMENTS="$2"
            shift 2
            ;;
        -m|--max-wait)
            MAX_WAIT="$2"
            shift 2
            ;;
        -i|--interval)
            INTERVAL="$2"
            shift 2
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
log_info "CDC Replication Latency Measurement"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Table: $TABLE"
echo "  Measurements: $MEASUREMENTS"
echo "  Max wait: ${MAX_WAIT}s per measurement"
echo "  Interval: ${INTERVAL}s between measurements"
echo ""

# Check services
log_info "Checking services..."
if ! check_services; then
    exit 1
fi
log_success "All services are healthy"
echo ""

# Perform measurements
log_info "Starting latency measurements..."
echo ""

latencies=()
successful=0
errors=0
timeouts=0

for ((i=1; i<=MEASUREMENTS; i++)); do
    printf "${BLUE}[%2d/%2d]${NC} " "$i" "$MEASUREMENTS"

    test_id=$(generate_test_id)

    case "$TABLE" in
        users)
            latency=$(measure_latency_users "$test_id")
            ;;
        products)
            latency=$(measure_latency_products "$test_id")
            ;;
        *)
            log_error "Unsupported table: $TABLE"
            exit 1
            ;;
    esac

    if [ "$latency" = "-1" ]; then
        echo -e "${RED}ERROR${NC} - Failed to insert data"
        ((errors++))
    elif [ "$latency" = "-2" ]; then
        echo -e "${RED}TIMEOUT${NC} - Data did not replicate within ${MAX_WAIT}s"
        ((timeouts++))
    else
        latencies+=("$latency")
        ((successful++))

        # Format latency output
        if [ $latency -lt 1000 ]; then
            echo -e "${GREEN}${latency}ms${NC}"
        elif [ $latency -lt 5000 ]; then
            printf "${YELLOW}%.2fs${NC}\n" "$(awk "BEGIN {print $latency / 1000}")"
        else
            printf "${RED}%.2fs${NC}\n" "$(awk "BEGIN {print $latency / 1000}")"
        fi
    fi

    # Wait between measurements (except for the last one)
    if [ $i -lt $MEASUREMENTS ]; then
        sleep "$INTERVAL"
    fi
done

echo ""

# Calculate statistics
if [ $successful -eq 0 ]; then
    log_error "No successful measurements"
    log_info "Check connector status: ./scripts/monitor-connectors.sh"
    exit 1
fi

log_info "==================================================================="
log_info "Latency Statistics"
log_info "==================================================================="
echo ""

# Sort latencies
IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}"))
unset IFS

# Calculate min, max, avg
min="${sorted[0]}"
max="${sorted[-1]}"
sum=0
for val in "${latencies[@]}"; do
    sum=$((sum + val))
done
avg=$((sum / successful))

# Calculate percentiles
p50=$(calculate_percentile 50 "${sorted[@]}")
p95=$(calculate_percentile 95 "${sorted[@]}")
p99=$(calculate_percentile 99 "${sorted[@]}")

# Display results
printf "  %-20s %10s\n" "Metric" "Value"
printf "  %-20s %10s\n" "--------------------" "----------"
printf "  %-20s %7dms\n" "Minimum" "$min"
printf "  %-20s %7dms\n" "Maximum" "$max"
printf "  %-20s %7dms\n" "Average" "$avg"
printf "  %-20s %7dms\n" "P50 (Median)" "$p50"
printf "  %-20s %7dms\n" "P95" "$p95"
printf "  %-20s %7dms\n" "P99" "$p99"
echo ""
printf "  %-20s %10d\n" "Successful" "$successful"
printf "  %-20s %10d\n" "Errors" "$errors"
printf "  %-20s %10d\n" "Timeouts" "$timeouts"
echo ""

# Assessment
if [ $avg -lt 1000 ]; then
    log_success "Excellent latency! Average under 1 second ✓"
elif [ $avg -lt 5000 ]; then
    log_success "Good latency! Average under 5 seconds ✓"
elif [ $avg -lt 10000 ]; then
    log_warning "Acceptable latency, but could be improved"
else
    log_warning "High latency detected - consider tuning"
    log_info "Optimization suggestions:"
    echo "  - Increase connector tasks (tasks.max in connector config)"
    echo "  - Adjust batch size settings"
    echo "  - Check Kafka broker performance"
    echo "  - Verify network latency between services"
fi

echo ""
log_info "For detailed monitoring:"
echo "  - Grafana: http://localhost:3000"
echo "  - Prometheus: http://localhost:9090"
echo "  - Connector status: ./scripts/monitor-connectors.sh"
