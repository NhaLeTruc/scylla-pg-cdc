#!/bin/bash
#
# Cleanup Test Environment
#
# Resets all test data and connector state to ensure clean test runs.
# This script should be run before executing contract or integration tests.
#

set -e

echo "========================================================================"
echo "SCYLLA-PG-CDC TEST ENVIRONMENT CLEANUP"
echo "========================================================================"
echo ""

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print step headers
print_step() {
    echo ""
    echo -e "${GREEN}[$1] $2${NC}"
    echo "------------------------------------------------------------------------"
}

# Function to print warnings
print_warning() {
    echo -e "${YELLOW}WARNING: $1${NC}"
}

# Function to print errors
print_error() {
    echo -e "${RED}ERROR: $1${NC}"
}

# Step 1: Truncate PostgreSQL tables
print_step "1/6" "Truncating PostgreSQL CDC tables..."

docker exec postgres psql -U postgres -d warehouse -c "
DO \$\$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'cdc_data')
    LOOP
        EXECUTE 'TRUNCATE TABLE cdc_data.' || quote_ident(r.tablename) || ' CASCADE';
        RAISE NOTICE 'Truncated cdc_data.%', r.tablename;
    END LOOP;
END \$\$;
" 2>/dev/null || print_warning "Failed to truncate PostgreSQL tables"

# Step 2: Truncate ScyllaDB tables
print_step "2/6" "Truncating ScyllaDB app_data tables..."

for table in users products orders order_items inventory_transactions; do
    docker exec scylla cqlsh -e "TRUNCATE app_data.$table;" 2>/dev/null && \
        echo "Truncated app_data.$table" || \
        print_warning "Failed to truncate app_data.$table"
done

# Step 3: Restart ScyllaDB CDC Source Connector
print_step "3/6" "Restarting ScyllaDB CDC Source Connector..."

curl -s -X POST http://localhost:8083/connectors/scylla-cdc-source/restart 2>/dev/null && \
    echo "ScyllaDB CDC Source connector restarted" || \
    print_warning "Failed to restart ScyllaDB CDC source connector"

sleep 5

# Check connector status
SCYLLA_STATUS=$(curl -s http://localhost:8083/connectors/scylla-cdc-source/status 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
echo "ScyllaDB CDC Source connector status: $SCYLLA_STATUS"

# Step 4: Restart PostgreSQL JDBC Sink Connector
print_step "4/6" "Restarting PostgreSQL JDBC Sink Connector..."

curl -s -X POST http://localhost:8083/connectors/postgres-jdbc-sink/restart 2>/dev/null && \
    echo "PostgreSQL JDBC Sink connector restarted" || \
    print_warning "Failed to restart PostgreSQL JDBC sink connector"

sleep 5

# Check connector status
POSTGRES_STATUS=$(curl -s http://localhost:8083/connectors/postgres-jdbc-sink/status 2>/dev/null | \
    python3 -c "import sys, json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
echo "PostgreSQL JDBC Sink connector status: $POSTGRES_STATUS"

# Step 5: Wait for Kafka lag to stabilize
print_step "5/6" "Waiting for Kafka consumer lag to stabilize..."

echo "Waiting 15 seconds for connectors to stabilize..."
sleep 15

# Check current lag
echo ""
echo "Current Kafka consumer lag:"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group connect-postgres-jdbc-sink --describe 2>/dev/null | \
    grep -E "GROUP|users|products|orders" | head -10 || \
    print_warning "Could not retrieve Kafka consumer lag"

# Step 6: Verification
print_step "6/6" "Verifying cleanup..."

echo ""
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

# Final summary
echo ""
echo "========================================================================"
if [ "$SCYLLA_STATUS" = "RUNNING" ] && [ "$POSTGRES_STATUS" = "RUNNING" ]; then
    echo -e "${GREEN}✓ CLEANUP SUCCESSFUL${NC}"
    echo ""
    echo "Both connectors are RUNNING and test data has been cleared."
    echo "You can now run contract and integration tests with a clean state."
    echo ""
    echo "Run tests with:"
    echo "  .venv/bin/pytest tests/contract/ -v"
else
    echo -e "${YELLOW}⚠ CLEANUP COMPLETED WITH WARNINGS${NC}"
    echo ""
    echo "Test data has been cleared, but one or more connectors may not be running."
    echo "ScyllaDB CDC Source: $SCYLLA_STATUS"
    echo "PostgreSQL JDBC Sink: $POSTGRES_STATUS"
    echo ""
    echo "Check connector logs:"
    echo "  docker logs kafka-connect"
fi
echo "========================================================================"
echo ""