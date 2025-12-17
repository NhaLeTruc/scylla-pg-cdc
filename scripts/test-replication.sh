#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Test Replication Script
# ==============================================================================
# Description: Insert test data in ScyllaDB and verify PostgreSQL replication
# Usage: ./scripts/test-replication.sh [OPTIONS]
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
TEST_ITERATIONS=1
WAIT_TIME=10
TABLE="users"
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

Test end-to-end CDC replication from ScyllaDB to PostgreSQL.

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (users, orders, products) (default: users)
    -n, --iterations N      Number of test iterations (default: 1)
    -w, --wait SECONDS      Wait time for replication (default: 10)
    --no-cleanup            Don't clean up test data after completion
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                              # Test users table with defaults
    $0 --table products --wait 15   # Test products table with 15s wait
    $0 --iterations 5               # Run 5 test iterations
    $0 --no-cleanup --verbose       # Keep test data and show debug output

DESCRIPTION:
    This script tests the CDC pipeline by:
    1. Inserting a test record in ScyllaDB
    2. Waiting for replication
    3. Verifying the record appears in PostgreSQL
    4. Optionally cleaning up test data
EOF
}

check_services() {
    log_info "Checking service health..."

    # Check ScyllaDB
    if ! docker exec scylla cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        log_error "ScyllaDB is not accessible"
        return 1
    fi
    log_verbose "ScyllaDB is healthy"

    # Check PostgreSQL
    if ! docker exec postgres pg_isready -U postgres -d warehouse &>/dev/null; then
        log_error "PostgreSQL is not accessible"
        return 1
    fi
    log_verbose "PostgreSQL is healthy"

    # Check Kafka Connect
    if ! curl -sf http://localhost:8083/ &>/dev/null; then
        log_error "Kafka Connect is not accessible"
        log_info "Deploy connectors with: ./scripts/deploy-connector.sh --all"
        return 1
    fi
    log_verbose "Kafka Connect is healthy"

    log_success "All services are healthy"
    return 0
}

generate_test_id() {
    # Generate a unique test ID using timestamp and random number
    echo "test-$(date +%s)-$RANDOM"
}

test_users_table() {
    local test_id=$1
    local test_email="test-${test_id}@example.com"
    local test_username="test_user_${test_id}"

    log_info "Testing users table replication..."
    log_verbose "Test ID: $test_id"
    log_verbose "Test email: $test_email"

    # Insert test data in ScyllaDB
    log_info "Step 1/3: Inserting test user in ScyllaDB..."
    docker exec scylla cqlsh -e "USE app_data; INSERT INTO users (user_id, username, email, first_name, last_name, created_at, updated_at, status) VALUES (uuid(), '${test_username}', '${test_email}', 'Test', 'User', toTimestamp(now()), toTimestamp(now()), 'active');" &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test user inserted in ScyllaDB"
    else
        log_error "Failed to insert test user in ScyllaDB"
        return 1
    fi

    # Verify in ScyllaDB
    log_verbose "Verifying in ScyllaDB..."
    local scylla_count
    scylla_count=$(docker exec scylla cqlsh -e "USE app_data; SELECT COUNT(*) FROM users WHERE email = '${test_email}';" 2>/dev/null | grep -E "^\s*[0-9]+" | tr -d ' ' || echo "0")

    log_verbose "ScyllaDB count: $scylla_count"

    # Wait for replication
    log_info "Step 2/3: Waiting ${WAIT_TIME}s for CDC replication..."
    sleep "$WAIT_TIME"

    # Verify in PostgreSQL
    log_info "Step 3/3: Verifying replication in PostgreSQL..."
    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "SELECT username, email, first_name, last_name, status FROM cdc_data.users WHERE email = '${test_email}';" 2>/dev/null)

    if [ -n "$pg_result" ] && [ "$pg_result" != " " ]; then
        log_success "Test user replicated to PostgreSQL!"
        if [ "$VERBOSE" = true ]; then
            echo "  $pg_result"
        fi

        # Cleanup if requested
        if [ "$CLEANUP" = true ]; then
            log_verbose "Cleaning up test data..."
            docker exec postgres psql -U postgres -d warehouse -c "DELETE FROM cdc_data.users WHERE email = '${test_email}';" &>/dev/null || true
            docker exec scylla cqlsh -e "USE app_data; DELETE FROM users WHERE email = '${test_email}' ALLOW FILTERING;" &>/dev/null || true
            log_verbose "Test data cleaned up"
        fi

        return 0
    else
        log_error "Test user NOT found in PostgreSQL"
        log_info "Check connector status: ./scripts/monitor-connectors.sh"
        return 1
    fi
}

test_products_table() {
    local test_id=$1
    local test_sku="TEST-${test_id}"

    log_info "Testing products table replication..."
    log_verbose "Test SKU: $test_sku"

    # Insert test data in ScyllaDB
    log_info "Step 1/3: Inserting test product in ScyllaDB..."
    docker exec scylla cqlsh -e "USE app_data; INSERT INTO products (product_id, sku, name, description, category, price, currency, stock_quantity, is_active, created_at, updated_at) VALUES (uuid(), '${test_sku}', 'Test Product', 'Test Description', 'Test', 9.99, 'USD', 100, true, toTimestamp(now()), toTimestamp(now()));" &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test product inserted in ScyllaDB"
    else
        log_error "Failed to insert test product in ScyllaDB"
        return 1
    fi

    # Wait for replication
    log_info "Step 2/3: Waiting ${WAIT_TIME}s for CDC replication..."
    sleep "$WAIT_TIME"

    # Verify in PostgreSQL
    log_info "Step 3/3: Verifying replication in PostgreSQL..."
    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "SELECT sku, name, category, price FROM cdc_data.products WHERE sku = '${test_sku}';" 2>/dev/null)

    if [ -n "$pg_result" ] && [ "$pg_result" != " " ]; then
        log_success "Test product replicated to PostgreSQL!"
        if [ "$VERBOSE" = true ]; then
            echo "  $pg_result"
        fi

        # Cleanup if requested
        if [ "$CLEANUP" = true ]; then
            log_verbose "Cleaning up test data..."
            docker exec postgres psql -U postgres -d warehouse -c "DELETE FROM cdc_data.products WHERE sku = '${test_sku}';" &>/dev/null || true
            docker exec scylla cqlsh -e "USE app_data; DELETE FROM products WHERE sku = '${test_sku}' ALLOW FILTERING;" &>/dev/null || true
            log_verbose "Test data cleaned up"
        fi

        return 0
    else
        log_error "Test product NOT found in PostgreSQL"
        return 1
    fi
}

test_orders_table() {
    local test_id=$1
    local test_order_num="TEST-ORD-${test_id}"

    # First, we need a user ID to create an order
    local user_id
    user_id=$(docker exec scylla cqlsh -e "USE app_data; SELECT user_id FROM users LIMIT 1;" 2>/dev/null | grep -E "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" | head -n1 | tr -d ' ')

    if [ -z "$user_id" ]; then
        log_warning "No users found in ScyllaDB, cannot test orders table"
        return 1
    fi

    log_info "Testing orders table replication..."
    log_verbose "Test order number: $test_order_num"
    log_verbose "Using user ID: $user_id"

    # Insert test data in ScyllaDB
    log_info "Step 1/3: Inserting test order in ScyllaDB..."
    docker exec scylla cqlsh -e "USE app_data; INSERT INTO orders (order_id, user_id, order_number, order_date, total_amount, currency, status, created_at, updated_at) VALUES (uuid(), ${user_id}, '${test_order_num}', toTimestamp(now()), 99.99, 'USD', 'pending', toTimestamp(now()), toTimestamp(now()));" &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test order inserted in ScyllaDB"
    else
        log_error "Failed to insert test order in ScyllaDB"
        return 1
    fi

    # Wait for replication
    log_info "Step 2/3: Waiting ${WAIT_TIME}s for CDC replication..."
    sleep "$WAIT_TIME"

    # Verify in PostgreSQL
    log_info "Step 3/3: Verifying replication in PostgreSQL..."
    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "SELECT order_number, status, total_amount FROM cdc_data.orders WHERE order_number = '${test_order_num}';" 2>/dev/null)

    if [ -n "$pg_result" ] && [ "$pg_result" != " " ]; then
        log_success "Test order replicated to PostgreSQL!"
        if [ "$VERBOSE" = true ]; then
            echo "  $pg_result"
        fi

        # Cleanup if requested
        if [ "$CLEANUP" = true ]; then
            log_verbose "Cleaning up test data..."
            docker exec postgres psql -U postgres -d warehouse -c "DELETE FROM cdc_data.orders WHERE order_number = '${test_order_num}';" &>/dev/null || true
            docker exec scylla cqlsh -e "USE app_data; DELETE FROM orders WHERE order_number = '${test_order_num}' ALLOW FILTERING;" &>/dev/null || true
            log_verbose "Test data cleaned up"
        fi

        return 0
    else
        log_error "Test order NOT found in PostgreSQL"
        return 1
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
        -n|--iterations)
            TEST_ITERATIONS="$2"
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
log_info "CDC Replication Test"
log_info "==================================================================="
echo ""

# Check services
if ! check_services; then
    exit 1
fi

echo ""

# Run test iterations
passed=0
failed=0

for ((i=1; i<=TEST_ITERATIONS; i++)); do
    if [ $TEST_ITERATIONS -gt 1 ]; then
        log_info "==================================================================="
        log_info "Test Iteration $i of $TEST_ITERATIONS"
        log_info "==================================================================="
        echo ""
    fi

    test_id=$(generate_test_id)

    case "$TABLE" in
        users)
            if test_users_table "$test_id"; then
                passed=$((passed + 1))
            else
                failed=$((failed + 1))
            fi
            ;;
        products)
            if test_products_table "$test_id"; then
                passed=$((passed + 1))
            else
                failed=$((failed + 1))
            fi
            ;;
        orders)
            if test_orders_table "$test_id"; then
                passed=$((passed + 1))
            else
                failed=$((failed + 1))
            fi
            ;;
        *)
            log_error "Unknown table: $TABLE"
            log_info "Supported tables: users, products, orders"
            exit 1
            ;;
    esac

    if [ $i -lt $TEST_ITERATIONS ]; then
        echo ""
        log_info "Waiting 2s before next iteration..."
        sleep 2
        echo ""
    fi
done

# Summary
echo ""
log_info "==================================================================="
log_info "Test Summary"
log_info "==================================================================="
echo ""
echo "  Table: $TABLE"
echo "  Iterations: $TEST_ITERATIONS"
echo "  Wait time: ${WAIT_TIME}s"
echo "  Passed: $passed"
echo "  Failed: $failed"
echo ""

if [ $failed -eq 0 ]; then
    log_success "All tests passed! ✓"
    log_info "CDC replication is working correctly"
    exit 0
else
    log_error "Some tests failed"
    log_info "Check connector status: ./scripts/monitor-connectors.sh"
    log_info "Check logs: docker compose -f docker/docker-compose.yml logs kafka-connect"
    exit 1
fi
