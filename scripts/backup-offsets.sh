#!/usr/bin/env bash

#
# Backup Kafka Connect Offsets
#
# This script backs up Kafka Connect connector offsets for disaster recovery.
# Offsets are stored in Kafka topics and backed up to JSON files.
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
BACKUP_DIR="${BACKUP_DIR:-/backups/offsets}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:9092}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
VERBOSE="${VERBOSE:-false}"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $1"
    fi
}

# Usage information
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Backup Kafka Connect connector offsets to JSON files.

OPTIONS:
    -c, --connector NAME    Backup specific connector (default: all connectors)
    -o, --output DIR        Output directory (default: $BACKUP_DIR)
    -r, --retention DAYS    Keep backups for N days (default: $RETENTION_DAYS)
    -v, --verbose           Verbose output
    -h, --help              Show this help message

EXAMPLES:
    # Backup all connectors
    $0

    # Backup specific connector
    $0 --connector scylla-source

    # Backup with custom retention
    $0 --retention 60

    # Restore from backup
    $0 --restore /backups/offsets/offsets-20250101-120000.json

ENVIRONMENT VARIABLES:
    KAFKA_CONNECT_URL       Kafka Connect REST API URL (default: http://localhost:8083)
    BACKUP_DIR              Backup directory (default: /backups/offsets)
    KAFKA_BROKER            Kafka broker (default: localhost:9092)
    RETENTION_DAYS          Backup retention days (default: 30)

EOF
    exit 0
}

# Check if Kafka Connect is accessible
check_kafka_connect() {
    log_info "Checking Kafka Connect availability..."

    if ! curl -sf "$KAFKA_CONNECT_URL" > /dev/null; then
        log_error "Kafka Connect is not accessible at $KAFKA_CONNECT_URL"
        exit 1
    fi

    log_success "Kafka Connect is accessible"
}

# Get list of connectors
get_connectors() {
    local connectors
    connectors=$(curl -sf "$KAFKA_CONNECT_URL/connectors" | jq -r '.[]')

    if [[ -z "$connectors" ]]; then
        log_warn "No connectors found"
        return 1
    fi

    echo "$connectors"
}

# Get connector offsets
get_connector_offsets() {
    local connector=$1
    local offsets_file=$2

    log_info "Fetching offsets for connector: $connector"

    # Get connector configuration
    local config
    config=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$connector/config")

    # Get connector status
    local status
    status=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$connector/status")

    # Get connector tasks
    local tasks
    tasks=$(curl -sf "$KAFKA_CONNECT_URL/connectors/$connector/tasks")

    # Create backup object
    local backup_data
    backup_data=$(jq -n \
        --arg connector "$connector" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson config "$config" \
        --argjson status "$status" \
        --argjson tasks "$tasks" \
        '{
            connector: $connector,
            backup_timestamp: $timestamp,
            config: $config,
            status: $status,
            tasks: $tasks
        }')

    # Get offset topic data (requires kafka-console-consumer)
    # This is a best-effort approach since Kafka Connect stores offsets internally
    log_debug "Fetching offset data from Kafka topics..."

    # Try to get offsets from connect-offsets topic
    local offset_data=""
    if command -v docker &> /dev/null; then
        offset_data=$(docker exec -i kafka kafka-console-consumer \
            --bootstrap-server "$KAFKA_BROKER" \
            --topic connect-offsets \
            --from-beginning \
            --max-messages 1000 \
            --timeout-ms 5000 \
            --property print.key=true \
            --property print.value=true 2>/dev/null | \
            grep -F "\"$connector\"" || echo "")
    fi

    if [[ -n "$offset_data" ]]; then
        backup_data=$(echo "$backup_data" | jq --arg offsets "$offset_data" '. + {offset_data: $offsets}')
    fi

    # Save to file
    echo "$backup_data" | jq '.' > "$offsets_file"

    log_success "Saved offsets to $offsets_file"
}

# Backup all connectors
backup_all_connectors() {
    local timestamp
    timestamp=$(date +%Y%m%d-%H%M%S)
    local backup_file="$BACKUP_DIR/offsets-$timestamp.json"

    log_info "Starting backup of all connectors..."

    local connectors
    connectors=$(get_connectors)

    if [[ -z "$connectors" ]]; then
        log_error "No connectors to backup"
        exit 1
    fi

    local backup_array="[]"

    while IFS= read -r connector; do
        log_info "Processing connector: $connector"

        local temp_file
        temp_file=$(mktemp)

        if get_connector_offsets "$connector" "$temp_file"; then
            backup_array=$(echo "$backup_array" | jq --slurpfile data "$temp_file" '. + $data')
        else
            log_warn "Failed to backup connector: $connector"
        fi

        rm -f "$temp_file"
    done <<< "$connectors"

    # Create final backup file
    local final_backup
    final_backup=$(jq -n \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --argjson connectors "$backup_array" \
        '{
            backup_timestamp: $timestamp,
            connector_count: ($connectors | length),
            connectors: $connectors
        }')

    echo "$final_backup" | jq '.' > "$backup_file"

    # Create 'latest' symlink
    ln -sf "$(basename "$backup_file")" "$BACKUP_DIR/offsets-latest.json"

    log_success "Backup completed: $backup_file"

    # Verify backup
    verify_backup "$backup_file"

    # Cleanup old backups
    cleanup_old_backups
}

# Backup specific connector
backup_connector() {
    local connector=$1
    local timestamp
    timestamp=$(date +%Y%m%d-%H%M%S)
    local backup_file="$BACKUP_DIR/offsets-$connector-$timestamp.json"

    log_info "Backing up connector: $connector"

    # Check if connector exists
    if ! curl -sf "$KAFKA_CONNECT_URL/connectors/$connector" > /dev/null; then
        log_error "Connector not found: $connector"
        exit 1
    fi

    # Get offsets
    get_connector_offsets "$connector" "$backup_file"

    # Create 'latest' symlink for this connector
    ln -sf "$(basename "$backup_file")" "$BACKUP_DIR/offsets-$connector-latest.json"

    log_success "Backup completed: $backup_file"

    # Verify backup
    verify_backup "$backup_file"
}

# Verify backup integrity
verify_backup() {
    local backup_file=$1

    log_info "Verifying backup integrity..."

    # Check file exists and is valid JSON
    if [[ ! -f "$backup_file" ]]; then
        log_error "Backup file not found: $backup_file"
        return 1
    fi

    if ! jq '.' "$backup_file" > /dev/null 2>&1; then
        log_error "Backup file is not valid JSON: $backup_file"
        return 1
    fi

    # Check required fields
    local has_timestamp
    has_timestamp=$(jq -r '.backup_timestamp' "$backup_file")

    if [[ "$has_timestamp" == "null" ]]; then
        log_error "Backup file missing backup_timestamp field"
        return 1
    fi

    # Calculate backup size
    local backup_size
    backup_size=$(du -h "$backup_file" | cut -f1)

    log_success "Backup verified successfully (size: $backup_size)"

    # Display summary
    if [[ "$VERBOSE" == "true" ]]; then
        log_info "Backup summary:"
        jq -r '
            "  Timestamp: \(.backup_timestamp)",
            "  Connectors: \(if .connector then 1 else .connector_count end)"
        ' "$backup_file"
    fi
}

# Cleanup old backups
cleanup_old_backups() {
    log_info "Cleaning up backups older than $RETENTION_DAYS days..."

    local deleted_count=0

    # Find and delete old backups
    while IFS= read -r old_file; do
        log_debug "Deleting old backup: $old_file"
        rm -f "$old_file"
        ((deleted_count++))
    done < <(find "$BACKUP_DIR" -name "offsets-*.json" -type f -mtime +"$RETENTION_DAYS")

    if [[ $deleted_count -gt 0 ]]; then
        log_success "Deleted $deleted_count old backup(s)"
    else
        log_info "No old backups to delete"
    fi
}

# List backups
list_backups() {
    log_info "Available backups in $BACKUP_DIR:"
    echo

    if [[ ! -d "$BACKUP_DIR" ]] || [[ -z "$(ls -A "$BACKUP_DIR"/offsets-*.json 2>/dev/null)" ]]; then
        log_warn "No backups found"
        return
    fi

    printf "%-30s %-20s %-10s %-s\n" "BACKUP FILE" "TIMESTAMP" "SIZE" "CONNECTORS"
    echo "--------------------------------------------------------------------------------"

    for backup in "$BACKUP_DIR"/offsets-*.json; do
        if [[ -L "$backup" ]]; then
            continue  # Skip symlinks
        fi

        local filename
        filename=$(basename "$backup")

        local timestamp
        timestamp=$(jq -r '.backup_timestamp' "$backup" 2>/dev/null || echo "unknown")

        local size
        size=$(du -h "$backup" | cut -f1)

        local connector_count
        connector_count=$(jq -r 'if .connector then 1 else .connector_count end' "$backup" 2>/dev/null || echo "unknown")

        printf "%-30s %-20s %-10s %-s\n" "$filename" "$timestamp" "$size" "$connector_count"
    done
}

# Restore from backup
restore_backup() {
    local backup_file=$1

    log_info "Restoring from backup: $backup_file"

    # Verify backup file
    if [[ ! -f "$backup_file" ]]; then
        log_error "Backup file not found: $backup_file"
        exit 1
    fi

    if ! verify_backup "$backup_file"; then
        log_error "Backup verification failed"
        exit 1
    fi

    # Warning about restoration
    log_warn "WARNING: Restoring offsets will require stopping and restarting connectors"
    read -p "Do you want to continue? (y/N): " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Restore cancelled"
        exit 0
    fi

    # Check if this is a single connector or all connectors backup
    local is_single_connector
    is_single_connector=$(jq -r 'has("connector")' "$backup_file")

    if [[ "$is_single_connector" == "true" ]]; then
        # Single connector restore
        local connector
        connector=$(jq -r '.connector' "$backup_file")

        log_info "Restoring connector: $connector"

        # Stop connector
        log_info "Stopping connector..."
        curl -sf -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector"
        sleep 5

        # Recreate connector with saved configuration
        log_info "Recreating connector..."
        local config
        config=$(jq '.config' "$backup_file")

        curl -sf -X POST "$KAFKA_CONNECT_URL/connectors" \
            -H "Content-Type: application/json" \
            -d "{\"name\": \"$connector\", \"config\": $config}"

        log_success "Connector restored: $connector"
    else
        # Multiple connectors restore
        local connector_count
        connector_count=$(jq -r '.connector_count' "$backup_file")

        log_info "Restoring $connector_count connector(s)..."

        # Restore each connector
        jq -c '.connectors[]' "$backup_file" | while read -r connector_data; do
            local connector
            connector=$(echo "$connector_data" | jq -r '.connector')

            log_info "Restoring connector: $connector"

            # Stop connector
            curl -sf -X DELETE "$KAFKA_CONNECT_URL/connectors/$connector" 2>/dev/null || true
            sleep 2

            # Recreate connector
            local config
            config=$(echo "$connector_data" | jq '.config')

            curl -sf -X POST "$KAFKA_CONNECT_URL/connectors" \
                -H "Content-Type: application/json" \
                -d "{\"name\": \"$connector\", \"config\": $config}"

            log_success "Restored connector: $connector"
            sleep 2
        done
    fi

    log_success "Restore completed successfully"
    log_info "Please verify connector status with: ./scripts/health-check.sh"
}

# Main function
main() {
    local connector=""
    local restore_file=""
    local list_only=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -c|--connector)
                connector="$2"
                shift 2
                ;;
            -o|--output)
                BACKUP_DIR="$2"
                mkdir -p "$BACKUP_DIR"
                shift 2
                ;;
            -r|--retention)
                RETENTION_DAYS="$2"
                shift 2
                ;;
            --restore)
                restore_file="$2"
                shift 2
                ;;
            --list)
                list_only=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    # List backups
    if [[ "$list_only" == "true" ]]; then
        list_backups
        exit 0
    fi

    # Restore from backup
    if [[ -n "$restore_file" ]]; then
        restore_backup "$restore_file"
        exit 0
    fi

    # Backup operation
    check_kafka_connect

    if [[ -n "$connector" ]]; then
        backup_connector "$connector"
    else
        backup_all_connectors
    fi

    log_success "All operations completed successfully"
}

# Run main function
main "$@"
