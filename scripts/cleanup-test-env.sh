#!/bin/bash
#
# Cleanup Test Environment
#
# Resets all test data and connector state to ensure clean test runs.
# This script should be run before executing contract or integration tests.
#
# Usage: ./scripts/cleanup-test-env.sh [OPTIONS]
#
# OPTIONS:
#   --full              Full cleanup including Kafka topics and consumer offsets
#   --keep-data         Keep database data, only restart connectors
#   -h, --help          Show this help message
#

set -e

# Configuration
FULL_CLEANUP=false
KEEP_DATA=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --full)
            FULL_CLEANUP=true
            shift
            ;;
        --keep-data)
            KEEP_DATA=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Resets test data and connector state for fresh test runs."
            echo ""
            echo "OPTIONS:"
            echo "  --full              Full cleanup including Kafka topics and consumer offsets"
            echo "  --keep-data         Keep database data, only restart connectors"
            echo "  -h, --help          Show this help message"
            echo ""
            echo "EXAMPLES:"
            echo "  $0                  # Standard cleanup (truncate data + restart connectors)"
            echo "  $0 --full           # Full cleanup (data + Kafka offsets + topics)"
            echo "  $0 --keep-data      # Only restart connectors, preserve data"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "========================================================================"
echo "SCYLLA-PG-CDC TEST ENVIRONMENT CLEANUP"
echo "========================================================================"
echo ""
echo "Mode: $([ "$FULL_CLEANUP" = true ] && echo "FULL CLEANUP" || ([ "$KEEP_DATA" = true ] && echo "CONNECTORS ONLY" || echo "STANDARD"))"
echo ""

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print step headers
print_step() {
    echo ""
    echo -e "${GREEN}[$1] $2${NC}"
    echo "------------------------------------------------------------------------"
}

# Function to print info
print_info() {
    echo -e "${BLUE}INFO: $1${NC}"
}

# Function to print warnings
print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

# Function to print errors
print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

# Determine total steps
TOTAL_STEPS=6
if [ "$FULL_CLEANUP" = true ]; then
    TOTAL_STEPS=8
fi

CURRENT_STEP=1

# Step 1: Truncate PostgreSQL tables (skip if --keep-data)
if [ "$KEEP_DATA" = false ]; then
    print_step "$CURRENT_STEP/$TOTAL_STEPS" "Truncating PostgreSQL CDC tables..."

    docker exec postgres psql -U postgres -d warehouse -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'cdc_data' AND tablename NOT IN ('replication_audit', 'data_quality_metrics'))
    LOOP
        EXECUTE 'TRUNCATE TABLE cdc_data.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Truncated cdc_data.%', r.tablename;
    END LOOP;
END \$\$;
" 2>/dev/null || print_warning "Failed to truncate PostgreSQL tables"

    CURRENT_STEP=$((CURRENT_STEP + 1))
else
    print_info "Skipping PostgreSQL truncation (--keep-data)"
fi

# Step 2: Truncate ScyllaDB tables (skip if --keep-data)
if [ "$KEEP_DATA" = false ]; then
    print_step "$CURRENT_STEP/$TOTAL_STEPS" "Truncating ScyllaDB app_data tables..."

    for table in users products orders order_items inventory_transactions; do
        docker exec scylla cqlsh -e "TRUNCATE app_data.$table;" 2>/dev/null && \
            echo "Truncated app_data.$table" || \
            print_warning "Failed to truncate app_data.$table"
    done

    CURRENT_STEP=$((CURRENT_STEP + 1))
else
    print_info "Skipping ScyllaDB truncation (--keep-data)"
fi

# Step 3: Delete and recreate connectors (if --full)
if [ "$FULL_CLEANUP" = true ]; then
    print_step "$CURRENT_STEP/$TOTAL_STEPS" "Deleting connectors for full cleanup..."

    curl -s -X DELETE http://localhost:8083/connectors/postgres-jdbc-sink 2>/dev/null && \
        echo "PostgreSQL JDBC Sink connector deleted" || \
        print_warning "Failed to delete PostgreSQL JDBC sink connector"

    curl -s -X DELETE http://localhost:8083/connectors/scylla-cdc-source 2>/dev/null && \
        echo "ScyllaDB CDC Source connector deleted" || \
        print_warning "Failed to delete ScyllaDB CDC source connector"

    sleep 3
    CURRENT_STEP=$((CURRENT_STEP + 1))
fi

# Step 4: Reset Kafka consumer offsets (if --full)
if [ "$FULL_CLEANUP" = true ]; then
    print_step "$CURRENT_STEP/$TOTAL_STEPS" "Resetting Kafka consumer group offsets..."

    print_info "Resetting offsets for connect-postgres-jdbc-sink consumer group..."
    docker exec kafka kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --group connect-postgres-jdbc-sink \
        --all-topics \
        --reset-offsets \
        --to-earliest \
        --execute 2>/dev/null && \
        echo "Consumer offsets reset to earliest" || \
        print_warning "Failed to reset consumer offsets (group may not exist yet)"

    CURRENT_STEP=$((CURRENT_STEP + 1))
fi

# Step 5: Restart or recreate ScyllaDB CDC Source Connector
print_step "$CURRENT_STEP/$TOTAL_STEPS" "$([ "$FULL_CLEANUP" = true ] && echo "Recreating" || echo "Restarting") ScyllaDB CDC Source Connector..."

if [ "$FULL_CLEANUP" = true ]; then
    curl -s -X POST -H "Content-Type: application/json" \
        --data @docker/kafka-connect/connectors/scylla-source.json \
        http://localhost:8083/connectors 2>/dev/null && \
        echo "ScyllaDB CDC Source connector created" || \
        print_warning "Failed to create ScyllaDB CDC source connector"
else
    curl -s -X POST http://localhost:8083/connectors/scylla-cdc-source/restart 2>/dev/null && \
        echo "ScyllaDB CDC Source connector restarted" || \
        print_warning "Failed to restart ScyllaDB CDC source connector"
fi

sleep 5

# Check connector status
SCYLLA_STATUS=$(curl -s http://localhost:8083/connectors/scylla-cdc-source/status 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
echo "ScyllaDB CDC Source connector status: $SCYLLA_STATUS"

CURRENT_STEP=$((CURRENT_STEP + 1))

# Step 6: Restart or recreate PostgreSQL JDBC Sink Connector
print_step "$CURRENT_STEP/$TOTAL_STEPS" "$([ "$FULL_CLEANUP" = true ] && echo "Recreating" || echo "Restarting") PostgreSQL JDBC Sink Connector..."

if [ "$FULL_CLEANUP" = true ]; then
    curl -s -X POST -H "Content-Type: application/json" \
        --data @docker/kafka-connect/connectors/postgres-sink.json \
        http://localhost:8083/connectors 2>/dev/null && \
        echo "PostgreSQL JDBC Sink connector created" || \
        print_warning "Failed to create PostgreSQL JDBC sink connector"
else
    curl -s -X POST http://localhost:8083/connectors/postgres-jdbc-sink/restart 2>/dev/null && \
        echo "PostgreSQL JDBC Sink connector restarted" || \
        print_warning "Failed to restart PostgreSQL JDBC sink connector"
fi

sleep 5

# Check connector status
POSTGRES_STATUS=$(curl -s http://localhost:8083/connectors/postgres-jdbc-sink/status 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
echo "PostgreSQL JDBC Sink connector status: $POSTGRES_STATUS"

CURRENT_STEP=$((CURRENT_STEP + 1))

# Step 7: Wait for Kafka lag to stabilize
print_step "$CURRENT_STEP/$TOTAL_STEPS" "Waiting for Kafka consumer lag to stabilize..."

echo "Waiting 15 seconds for connectors to stabilize..."
sleep 15

# Check current lag
echo ""
echo "Current Kafka consumer lag:"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group connect-postgres-jdbc-sink --describe 2>/dev/null | \
    grep -E "GROUP|users|products|orders" | head -10 || \
    print_warning "Could not retrieve Kafka consumer lag"

CURRENT_STEP=$((CURRENT_STEP + 1))

# Step 8: Verification
print_step "$CURRENT_STEP/$TOTAL_STEPS" "Verifying cleanup..."

echo ""
if [ "$KEEP_DATA" = false ]; then
    echo "PostgreSQL row counts:"
    docker exec postgres psql -U postgres -d warehouse -c "
        SELECT 'users' as table_name, COUNT(*) as row_count FROM cdc_data.users
        UNION ALL
        SELECT 'products', COUNT(*) FROM cdc_data.products
        UNION ALL
        SELECT 'orders', COUNT(*) FROM cdc_data.orders
        UNION ALL
        SELECT 'order_items', COUNT(*) FROM cdc_data.order_items
        UNION ALL
        SELECT 'inventory_transactions', COUNT(*) FROM cdc_data.inventory_transactions
        ORDER BY table_name;
    " 2>/dev/null || print_warning "Could not verify PostgreSQL tables"

    echo ""
    echo "ScyllaDB row counts:"
    for table in users products orders order_items inventory_transactions; do
        COUNT=$(docker exec scylla cqlsh -e "SELECT COUNT(*) FROM app_data.$table;" 2>/dev/null | \
            grep -oP '\d+' | head -1 || echo "?")
        printf "  %-25s %s\n" "app_data.$table:" "$COUNT rows"
    done
else
    print_info "Data verification skipped (--keep-data mode)"
fi

# Final summary
echo ""
echo "========================================================================"
if [ "$SCYLLA_STATUS" = "RUNNING" ] && [ "$POSTGRES_STATUS" = "RUNNING" ]; then
    echo -e "${GREEN}✓ CLEANUP SUCCESSFUL${NC}"
    echo ""
    echo "Connectors: RUNNING"
    echo "Data: $([ "$KEEP_DATA" = true ] && echo "PRESERVED" || echo "CLEARED")"
    echo "Consumer offsets: $([ "$FULL_CLEANUP" = true ] && echo "RESET" || echo "PRESERVED")"
    echo ""
    echo "Environment ready for testing!"
    echo ""
    if [ "$FULL_CLEANUP" = true ]; then
        print_info "Full cleanup performed - all Kafka offsets reset to earliest"
        print_info "CDC pipeline will reprocess all messages from the beginning"
    fi
    echo ""
    echo "Run tests with:"
    echo "  .venv/bin/pytest tests/contract/ -v"
    echo ""
    echo "Quick test CDC pipeline:"
    echo "  ./scripts/test-replication.sh --table users"
else
    echo -e "${YELLOW}⚠ CLEANUP COMPLETED WITH WARNINGS${NC}"
    echo ""
    echo "Test data has been processed, but connectors may need attention."
    echo "ScyllaDB CDC Source: $SCYLLA_STATUS"
    echo "PostgreSQL JDBC Sink: $POSTGRES_STATUS"
    echo ""
    echo "Check connector logs:"
    echo "  docker logs kafka-connect --tail 100"
    echo ""
    echo "Check connector status:"
    echo "  ./scripts/monitor-connectors.sh"
fi
echo "========================================================================"
echo ""