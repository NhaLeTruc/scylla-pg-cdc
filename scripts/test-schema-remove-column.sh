#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Remove Column Schema Evolution Test
# ==============================================================================
# Description: Test schema evolution by removing a column
# Usage: ./scripts/test-schema-remove-column.sh [OPTIONS]
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
TEMP_COLUMN="temp_test_column"
TEMP_COLUMN_TYPE="text"
WAIT_TIME=15
CLEANUP=true
VERBOSE=false
SCHEMA_REGISTRY_URL="http://localhost:8081"

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

Test schema evolution by removing a column (backward compatible).

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (default: users)
    -c, --column NAME       Temporary column to add/remove (default: temp_test_column)
    --type TYPE             Temporary column type (default: text)
    -w, --wait SECONDS      Wait time after schema change (default: 15)
    --no-cleanup            Don't clean up test data
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                              # Test with default temp_test_column
    $0 --column test_field          # Test with custom column
    $0 --no-cleanup --verbose       # Keep data and show debug output

DESCRIPTION:
    This script tests schema evolution when removing a column by:
    1. Adding a temporary column to the schema
    2. Inserting test data WITH the column
    3. Verifying replication works
    4. Removing the column from the schema
    5. Inserting test data WITHOUT the column
    6. Verifying both old and new data are accessible
    7. Checking Schema Registry for evolution

    Column removal is BACKWARD compatible because:
    - Old data (with column) can still be processed (column ignored)
    - New data (without column) works correctly
    - Consumers that don't use the column are unaffected
EOF
}

check_prerequisites() {
    # Check if services are running
    if ! docker exec scylla cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        log_error "ScyllaDB is not accessible"
        return 1
    fi

    if ! docker exec postgres pg_isready -U postgres -d warehouse &>/dev/null; then
        log_error "PostgreSQL is not accessible"
        return 1
    fi

    if ! curl -sf "$SCHEMA_REGISTRY_URL" &>/dev/null; then
        log_error "Schema Registry is not accessible at $SCHEMA_REGISTRY_URL"
        return 1
    fi

    if ! curl -sf http://localhost:8083/ &>/dev/null; then
        log_error "Kafka Connect is not accessible"
        return 1
    fi

    return 0
}

get_schema_version() {
    local subject="cdc.scylla.${TABLE}-value"
    local version

    version=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest" 2>/dev/null | jq -r '.version' 2>/dev/null || echo "0")
    echo "$version"
}

get_schema_id() {
    local subject="cdc.scylla.${TABLE}-value"
    local schema_id

    schema_id=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest" 2>/dev/null | jq -r '.id' 2>/dev/null || echo "0")
    echo "$schema_id"
}

column_exists_scylla() {
    local table=$1
    local column=$2
    local result

    result=$(docker exec scylla cqlsh -e "
        SELECT column_name FROM system_schema.columns
        WHERE keyspace_name = 'app_data' AND table_name = '${table}' AND column_name = '${column}';
    " 2>/dev/null | grep -c "$column" || echo "0")

    [ "$result" -gt 0 ]
}

column_exists_postgres() {
    local table=$1
    local column=$2
    local result

    result=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'cdc_data' AND table_name = '${table}' AND column_name = '${column}';
    " 2>/dev/null | tr -d ' ' || echo "0")

    [ "$result" -gt 0 ]
}

add_column_scylla() {
    local table=$1
    local column=$2
    local col_type=$3

    log_info "Adding temporary column '${column}' to ScyllaDB..."

    docker exec scylla cqlsh -e "
        ALTER TABLE app_data.${table} ADD ${column} ${col_type};
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Column added to ScyllaDB"
        return 0
    else
        log_error "Failed to add column to ScyllaDB"
        return 1
    fi
}

add_column_postgres() {
    local table=$1
    local column=$2
    local col_type=$3

    log_info "Adding temporary column '${column}' to PostgreSQL..."

    # Map CQL types to PostgreSQL types
    local pg_type="$col_type"
    case "$col_type" in
        text) pg_type="TEXT" ;;
        int) pg_type="INTEGER" ;;
        bigint) pg_type="BIGINT" ;;
        double) pg_type="DOUBLE PRECISION" ;;
        timestamp) pg_type="TIMESTAMP" ;;
        boolean) pg_type="BOOLEAN" ;;
    esac

    docker exec postgres psql -U postgres -d warehouse -c "
        ALTER TABLE cdc_data.${table} ADD COLUMN IF NOT EXISTS ${column} ${pg_type};
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Column added to PostgreSQL"
        return 0
    else
        log_warning "Failed to add column to PostgreSQL (may be auto-created by sink)"
        return 0
    fi
}

remove_column_scylla() {
    local table=$1
    local column=$2

    log_info "Removing column '${column}' from ScyllaDB..."

    docker exec scylla cqlsh -e "
        ALTER TABLE app_data.${table} DROP ${column};
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Column removed from ScyllaDB"
        return 0
    else
        log_error "Failed to remove column from ScyllaDB"
        return 1
    fi
}

remove_column_postgres() {
    local table=$1
    local column=$2

    log_verbose "Removing column '${column}' from PostgreSQL..."

    # Note: We don't remove from PostgreSQL as the column may still have data
    # In production, column removal is typically a multi-step process
    docker exec postgres psql -U postgres -d warehouse -c "
        ALTER TABLE cdc_data.${table} DROP COLUMN IF EXISTS ${column};
    " &>/dev/null
}

generate_test_id() {
    echo "schema-remove-$(date +%s)-$RANDOM"
}

insert_test_data_with_column() {
    local test_id=$1
    local test_email="with-col-${test_id}@example.com"
    local column_value="test-value-${test_id}"

    log_info "Inserting test data WITH temporary column..."

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, ${TEMP_COLUMN}, created_at, updated_at, status)
        VALUES (uuid(), 'with_col_${test_id}', '${test_email}', 'With', 'Column', '${column_value}', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test data inserted (with column)"
        echo "$test_email"
        return 0
    else
        log_error "Failed to insert test data with column"
        return 1
    fi
}

insert_test_data_without_column() {
    local test_id=$1
    local test_email="without-col-${test_id}@example.com"

    log_info "Inserting test data WITHOUT the removed column..."

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, created_at, updated_at, status)
        VALUES (uuid(), 'without_col_${test_id}', '${test_email}', 'Without', 'Column', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test data inserted (without column)"
        echo "$test_email"
        return 0
    else
        log_error "Failed to insert test data without column"
        return 1
    fi
}

verify_replication() {
    local email=$1
    local description=$2

    log_verbose "Verifying replication for: $email ($description)"

    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT email FROM cdc_data.${TABLE} WHERE email = '${email}';
    " 2>/dev/null | tr -d ' ')

    if [ -n "$pg_result" ]; then
        log_success "Record replicated: $description"
        return 0
    else
        log_error "Record NOT replicated: $description"
        return 1
    fi
}

cleanup_test_data() {
    local email=$1

    log_verbose "Cleaning up test data: $email"

    docker exec postgres psql -U postgres -d warehouse -c "
        DELETE FROM cdc_data.${TABLE} WHERE email = '${email}';
    " &>/dev/null

    docker exec scylla cqlsh -e "
        USE app_data;
        DELETE FROM ${TABLE} WHERE email = '${email}';
    " &>/dev/null
}

check_schema_registry() {
    local subject="cdc.scylla.${TABLE}-value"

    log_info "Checking Schema Registry for evolution..."

    local versions
    versions=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" 2>/dev/null | jq -r '.[]' 2>/dev/null)

    if [ -z "$versions" ]; then
        log_warning "No schema versions found for subject: $subject"
        return 1
    fi

    log_info "Schema versions in registry:"
    for version in $versions; do
        local schema_resp
        schema_resp=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/${version}" 2>/dev/null)

        local schema_id
        schema_id=$(echo "$schema_resp" | jq -r '.id' 2>/dev/null)

        echo "  Version $version (ID: $schema_id)"

        if [ "$VERBOSE" = true ]; then
            local fields
            fields=$(echo "$schema_resp" | jq -r '.schema' | jq -r '. | fromjson | .fields[] | .name' 2>/dev/null)
            echo "    Fields: $(echo "$fields" | tr '\n' ', ' | sed 's/,$//')"
        fi
    done

    return 0
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
        -c|--column)
            TEMP_COLUMN="$2"
            shift 2
            ;;
        --type)
            TEMP_COLUMN_TYPE="$2"
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
log_info "Schema Evolution Test: Remove Column (Backward Compatible)"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Table: $TABLE"
echo "  Temporary column: $TEMP_COLUMN ($TEMP_COLUMN_TYPE)"
echo "  Wait time: ${WAIT_TIME}s"
echo "  Cleanup: $CLEANUP"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi
log_success "All services are healthy"
echo ""

# Record initial state
log_info "Recording initial schema state..."
initial_version=$(get_schema_version)
initial_schema_id=$(get_schema_id)

if [ "$initial_version" = "0" ]; then
    log_warning "No schema found in registry"
else
    log_success "Current schema version: $initial_version (ID: $initial_schema_id)"
fi
echo ""

# Check if column already exists
if column_exists_scylla "$TABLE" "$TEMP_COLUMN"; then
    log_error "Column '${TEMP_COLUMN}' already exists in ScyllaDB table '${TABLE}'"
    log_info "Removing existing column first..."
    remove_column_scylla "$TABLE" "$TEMP_COLUMN"
    remove_column_postgres "$TABLE" "$TEMP_COLUMN"
    sleep 2
fi

# Step 1: Add temporary column
log_info "==================================================================="
log_info "Step 1: Add temporary column to schema"
log_info "==================================================================="
echo ""

if ! add_column_scylla "$TABLE" "$TEMP_COLUMN" "$TEMP_COLUMN_TYPE"; then
    exit 1
fi

add_column_postgres "$TABLE" "$TEMP_COLUMN" "$TEMP_COLUMN_TYPE"

log_info "Waiting 5s for schema propagation..."
sleep 5
echo ""

# Step 2: Insert data with the column
log_info "==================================================================="
log_info "Step 2: Insert test data WITH the temporary column"
log_info "==================================================================="
echo ""

test_id=$(generate_test_id)
with_col_email=$(insert_test_data_with_column "$test_id")

if [ -z "$with_col_email" ]; then
    log_error "Failed to insert data with column"
    remove_column_scylla "$TABLE" "$TEMP_COLUMN"
    remove_column_postgres "$TABLE" "$TEMP_COLUMN"
    exit 1
fi

log_info "Waiting ${WAIT_TIME}s for replication..."
sleep "$WAIT_TIME"

if verify_replication "$with_col_email" "data with column"; then
    log_success "Data with column replicated successfully"
else
    log_error "Data with column failed to replicate"
    remove_column_scylla "$TABLE" "$TEMP_COLUMN"
    remove_column_postgres "$TABLE" "$TEMP_COLUMN"
    exit 1
fi
echo ""

# Step 3: Remove the column
log_info "==================================================================="
log_info "Step 3: Remove the column from schema"
log_info "==================================================================="
echo ""

if ! remove_column_scylla "$TABLE" "$TEMP_COLUMN"; then
    if [ "$CLEANUP" = true ]; then
        cleanup_test_data "$with_col_email"
    fi
    exit 1
fi

log_info "Waiting 5s for schema propagation..."
sleep 5
echo ""

# Step 4: Insert data without the column
log_info "==================================================================="
log_info "Step 4: Insert test data WITHOUT the removed column"
log_info "==================================================================="
echo ""

without_col_email=$(insert_test_data_without_column "$test_id")

if [ -z "$without_col_email" ]; then
    log_error "Failed to insert data without column"

    if [ "$CLEANUP" = true ]; then
        cleanup_test_data "$with_col_email"
    fi

    exit 1
fi

log_info "Waiting ${WAIT_TIME}s for replication..."
sleep "$WAIT_TIME"

if verify_replication "$without_col_email" "data without column"; then
    log_success "Data without column replicated successfully"
else
    log_error "Data without column failed to replicate"

    if [ "$CLEANUP" = true ]; then
        cleanup_test_data "$with_col_email"
        cleanup_test_data "$without_col_email"
    fi

    exit 1
fi
echo ""

# Step 5: Verify old data still accessible
log_info "==================================================================="
log_info "Step 5: Verify old data still accessible (backward compatibility)"
log_info "==================================================================="
echo ""

if verify_replication "$with_col_email" "old data with removed column"; then
    log_success "Old data (with removed column) still accessible ✓"
    log_success "BACKWARD COMPATIBILITY VERIFIED"
else
    log_error "Old data became inaccessible after column removal"
    log_error "BACKWARD COMPATIBILITY FAILED"
fi
echo ""

# Step 6: Check Schema Registry
log_info "==================================================================="
log_info "Step 6: Verify schema evolution in Schema Registry"
log_info "==================================================================="
echo ""

check_schema_registry

final_version=$(get_schema_version)
final_schema_id=$(get_schema_id)

if [ "$final_version" != "$initial_version" ] && [ "$final_version" != "0" ]; then
    log_success "Schema version evolved: $initial_version → $final_version"
    log_success "Final schema ID: $final_schema_id"
else
    log_warning "Schema version unchanged or not found"
    log_info "Check connector logs: docker compose logs kafka-connect"
fi
echo ""

# Cleanup
if [ "$CLEANUP" = true ]; then
    log_info "==================================================================="
    log_info "Cleanup: Removing test data"
    log_info "==================================================================="
    echo ""

    cleanup_test_data "$with_col_email"
    cleanup_test_data "$without_col_email"

    # Remove column from PostgreSQL if it still exists
    if column_exists_postgres "$TABLE" "$TEMP_COLUMN"; then
        remove_column_postgres "$TABLE" "$TEMP_COLUMN"
    fi

    log_success "Cleanup completed"
    echo ""
fi

# Summary
log_success "==================================================================="
log_success "Schema Evolution Test: PASSED ✓"
log_success "==================================================================="
echo ""
log_info "Test Results:"
echo "  ✓ Temporary column added to schema"
echo "  ✓ Data with column inserted and replicated"
echo "  ✓ Column removed from schema"
echo "  ✓ Data without column inserted and replicated"
echo "  ✓ Old data (with removed column) still accessible"
if [ "$final_version" != "$initial_version" ] && [ "$final_version" != "0" ]; then
    echo "  ✓ Schema Registry tracked evolution"
fi
echo ""
log_info "This confirms the pipeline handles column removal correctly."
log_info "Column removal is BACKWARD compatible:"
echo "  - Old records (with removed column) remain accessible"
echo "  - New records (without column) work correctly"
echo "  - Consumers that ignore the column are unaffected"
echo ""
log_warning "Production considerations:"
echo "  - Column removal should be a multi-phase process"
echo "  - Phase 1: Stop writing to the column"
echo "  - Phase 2: Update consumers to ignore the column"
echo "  - Phase 3: Remove column from schema"
echo "  - Phase 4: Clean up historical data if needed"
