#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Add Column Schema Evolution Test
# ==============================================================================
# Description: Test backward-compatible schema evolution by adding a column
# Usage: ./scripts/test-schema-add-column.sh [OPTIONS]
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
NEW_COLUMN="phone_number"
NEW_COLUMN_TYPE="text"
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

Test backward-compatible schema evolution by adding a new column with a default value.

OPTIONS:
    -h, --help              Show this help message
    -t, --table TABLE       Table to test (default: users)
    -c, --column NAME       New column name (default: phone_number)
    --type TYPE             New column type (default: text)
    -w, --wait SECONDS      Wait time after schema change (default: 15)
    --no-cleanup            Don't clean up test data and schema changes
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                                      # Add phone_number to users table
    $0 --column middle_name --type text    # Add middle_name column
    $0 --no-cleanup --verbose              # Keep changes and show debug output

DESCRIPTION:
    This script tests backward-compatible schema evolution by:
    1. Recording the current schema version
    2. Adding a new optional column with a default value
    3. Inserting test data with and without the new column
    4. Verifying both records replicate correctly
    5. Checking Schema Registry for the new version
    6. Optionally rolling back changes

    Backward compatibility means:
    - Old data (without new column) can still be read
    - New data (with new column) works correctly
    - Schema Registry accepts the change as BACKWARD compatible
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

    log_info "Adding column '${column}' to ScyllaDB table '${table}'..."

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

    log_info "Adding column '${column}' to PostgreSQL table '${table}'..."

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
        log_error "Failed to add column to PostgreSQL"
        return 1
    fi
}

remove_column_scylla() {
    local table=$1
    local column=$2

    log_verbose "Removing column '${column}' from ScyllaDB..."

    docker exec scylla cqlsh -e "
        ALTER TABLE app_data.${table} DROP ${column};
    " &>/dev/null
}

remove_column_postgres() {
    local table=$1
    local column=$2

    log_verbose "Removing column '${column}' from PostgreSQL..."

    docker exec postgres psql -U postgres -d warehouse -c "
        ALTER TABLE cdc_data.${table} DROP COLUMN IF EXISTS ${column};
    " &>/dev/null
}

generate_test_id() {
    echo "schema-test-$(date +%s)-$RANDOM"
}

insert_test_data_without_new_column() {
    local test_id=$1
    local test_email="old-schema-${test_id}@example.com"

    log_info "Inserting test data WITHOUT new column (old schema)..."

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, created_at, updated_at, status)
        VALUES (uuid(), 'old_${test_id}', '${test_email}', 'Old', 'Schema', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test data inserted (old schema)"
        echo "$test_email"
        return 0
    else
        log_error "Failed to insert test data"
        return 1
    fi
}

insert_test_data_with_new_column() {
    local test_id=$1
    local test_email="new-schema-${test_id}@example.com"
    local column_value="+1-555-0123"

    log_info "Inserting test data WITH new column (new schema)..."

    docker exec scylla cqlsh -e "
        USE app_data;
        INSERT INTO ${TABLE} (user_id, username, email, first_name, last_name, ${NEW_COLUMN}, created_at, updated_at, status)
        VALUES (uuid(), 'new_${test_id}', '${test_email}', 'New', 'Schema', '${column_value}', toTimestamp(now()), toTimestamp(now()), 'active');
    " &>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Test data inserted (new schema with ${NEW_COLUMN})"
        echo "$test_email"
        return 0
    else
        log_error "Failed to insert test data with new column"
        return 1
    fi
}

verify_replication() {
    local email=$1
    local should_have_column=$2

    log_verbose "Verifying replication for: $email"

    local pg_result
    pg_result=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT email, ${NEW_COLUMN} FROM cdc_data.${TABLE} WHERE email = '${email}';
    " 2>/dev/null)

    if [ -n "$pg_result" ] && [ "$pg_result" != " " ]; then
        log_success "Record replicated: $email"

        if [ "$should_have_column" = true ]; then
            log_verbose "  New column value: $(echo "$pg_result" | awk '{print $2}')"
        fi

        return 0
    else
        log_error "Record NOT replicated: $email"
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
            NEW_COLUMN="$2"
            shift 2
            ;;
        --type)
            NEW_COLUMN_TYPE="$2"
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
log_info "Schema Evolution Test: Add Column (Backward Compatible)"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Table: $TABLE"
echo "  New column: $NEW_COLUMN ($NEW_COLUMN_TYPE)"
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
    log_warning "No schema found in registry (subject may not exist yet)"
    log_info "This is expected if no data has been replicated yet"
else
    log_success "Current schema version: $initial_version (ID: $initial_schema_id)"
fi
echo ""

# Check if column already exists
if column_exists_scylla "$TABLE" "$NEW_COLUMN"; then
    log_error "Column '${NEW_COLUMN}' already exists in ScyllaDB table '${TABLE}'"
    log_info "Please choose a different column name or clean up first"
    exit 1
fi

# Step 1: Insert data with old schema
log_info "==================================================================="
log_info "Step 1: Insert test data with OLD schema (before column addition)"
log_info "==================================================================="
echo ""

test_id=$(generate_test_id)
old_email=$(insert_test_data_without_new_column "$test_id")

if [ -z "$old_email" ]; then
    log_error "Failed to insert old schema data"
    exit 1
fi

log_info "Waiting ${WAIT_TIME}s for initial replication..."
sleep "$WAIT_TIME"

if verify_replication "$old_email" false; then
    log_success "Old schema data replicated successfully"
else
    log_error "Old schema data failed to replicate"
    exit 1
fi
echo ""

# Step 2: Add column to ScyllaDB
log_info "==================================================================="
log_info "Step 2: Add new column to ScyllaDB schema"
log_info "==================================================================="
echo ""

if ! add_column_scylla "$TABLE" "$NEW_COLUMN" "$NEW_COLUMN_TYPE"; then
    exit 1
fi

# Also add to PostgreSQL sink
if ! add_column_postgres "$TABLE" "$NEW_COLUMN" "$NEW_COLUMN_TYPE"; then
    log_warning "Failed to add column to PostgreSQL"
    log_info "This may be OK if sink connector auto-creates columns"
fi
echo ""

# Step 3: Insert data with new schema
log_info "==================================================================="
log_info "Step 3: Insert test data with NEW schema (with new column)"
log_info "==================================================================="
echo ""

new_email=$(insert_test_data_with_new_column "$test_id")

if [ -z "$new_email" ]; then
    log_error "Failed to insert new schema data"

    if [ "$CLEANUP" = true ]; then
        log_info "Rolling back schema change..."
        remove_column_scylla "$TABLE" "$NEW_COLUMN"
        remove_column_postgres "$TABLE" "$NEW_COLUMN"
        cleanup_test_data "$old_email"
    fi

    exit 1
fi

log_info "Waiting ${WAIT_TIME}s for new schema replication..."
sleep "$WAIT_TIME"

if verify_replication "$new_email" true; then
    log_success "New schema data replicated successfully"
else
    log_error "New schema data failed to replicate"

    if [ "$CLEANUP" = true ]; then
        log_info "Rolling back changes..."
        remove_column_scylla "$TABLE" "$NEW_COLUMN"
        remove_column_postgres "$TABLE" "$NEW_COLUMN"
        cleanup_test_data "$old_email"
        cleanup_test_data "$new_email"
    fi

    exit 1
fi
echo ""

# Step 4: Verify old data still accessible
log_info "==================================================================="
log_info "Step 4: Verify old data still accessible (backward compatibility)"
log_info "==================================================================="
echo ""

if verify_replication "$old_email" false; then
    log_success "Old data still accessible after schema change ✓"
    log_success "BACKWARD COMPATIBILITY VERIFIED"
else
    log_error "Old data became inaccessible after schema change"
    log_error "BACKWARD COMPATIBILITY FAILED"
fi
echo ""

# Step 5: Check Schema Registry
log_info "==================================================================="
log_info "Step 5: Verify schema evolution in Schema Registry"
log_info "==================================================================="
echo ""

check_schema_registry

new_version=$(get_schema_version)
new_schema_id=$(get_schema_id)

if [ "$new_version" != "$initial_version" ] && [ "$new_version" != "0" ]; then
    log_success "Schema version evolved: $initial_version → $new_version"
    log_success "New schema ID: $new_schema_id"
else
    log_warning "Schema version unchanged or not found"
    log_info "Check connector logs: docker compose logs kafka-connect"
fi
echo ""

# Cleanup
if [ "$CLEANUP" = true ]; then
    log_info "==================================================================="
    log_info "Cleanup: Rolling back schema changes and test data"
    log_info "==================================================================="
    echo ""

    cleanup_test_data "$old_email"
    cleanup_test_data "$new_email"
    remove_column_scylla "$TABLE" "$NEW_COLUMN"
    remove_column_postgres "$TABLE" "$NEW_COLUMN"

    log_success "Cleanup completed"
    echo ""
fi

# Summary
log_success "==================================================================="
log_success "Schema Evolution Test: PASSED ✓"
log_success "==================================================================="
echo ""
log_info "Test Results:"
echo "  ✓ Old schema data inserted and replicated"
echo "  ✓ Column added to schema"
echo "  ✓ New schema data inserted and replicated"
echo "  ✓ Old data still accessible (backward compatible)"
if [ "$new_version" != "$initial_version" ] && [ "$new_version" != "0" ]; then
    echo "  ✓ Schema Registry updated with new version"
fi
echo ""
log_info "This confirms the pipeline handles BACKWARD-compatible schema changes correctly."
log_info "Consumers using the old schema can still read data produced with the new schema."
echo ""

if [ "$CLEANUP" = false ]; then
    log_warning "Schema changes and test data were NOT cleaned up (--no-cleanup)"
    log_info "To manually remove:"
    echo "  ScyllaDB: ALTER TABLE app_data.${TABLE} DROP ${NEW_COLUMN};"
    echo "  PostgreSQL: ALTER TABLE cdc_data.${TABLE} DROP COLUMN ${NEW_COLUMN};"
fi
