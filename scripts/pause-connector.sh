#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Pause Connector Script
# ==============================================================================
# Description: Gracefully pause a running Kafka Connect connector
# Usage: ./scripts/pause-connector.sh [OPTIONS]
# ==============================================================================

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME=""
WAIT_PAUSED=true
ALL_CONNECTORS=false

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

Gracefully pause a Kafka Connect connector.

OPTIONS:
    -h, --help              Show this help message
    -n, --name NAME         Connector name to pause
    -c, --connect URL       Kafka Connect REST API URL (default: $CONNECT_URL)
    --no-wait               Don't wait for connector to pause
    --all                   Pause all connectors

EXAMPLES:
    $0 --name scylla-source
    $0 --name postgres-sink
    $0 --all

DESCRIPTION:
    Pauses a running connector, which stops it from reading/writing data
    but preserves its configuration and offsets. Resume with resume-connector.sh.
EOF
}

get_connector_status() {
    local connector_name=$1
    curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN"
}

pause_connector() {
    local connector_name=$1
    local current_status

    current_status=$(get_connector_status "$connector_name")

    if [ "$current_status" = "UNKNOWN" ]; then
        log_error "Connector not found: $connector_name"
        return 1
    fi

    if [ "$current_status" = "PAUSED" ]; then
        log_info "Connector $connector_name is already paused"
        return 0
    fi

    log_info "Pausing connector: $connector_name (current status: $current_status)"

    if curl -sf -X PUT "${CONNECT_URL}/connectors/${connector_name}/pause" &>/dev/null; then
        log_success "Pause request sent successfully"

        if [ "$WAIT_PAUSED" = true ]; then
            wait_for_pause "$connector_name"
        fi
    else
        log_error "Failed to pause connector"
        return 1
    fi
}

wait_for_pause() {
    local connector_name=$1
    local max_wait=30
    local elapsed=0
    local status

    log_info "Waiting for connector to pause..."

    while [ $elapsed -lt $max_wait ]; do
        status=$(get_connector_status "$connector_name")

        if [ "$status" = "PAUSED" ]; then
            log_success "Connector $connector_name is now paused"
            return 0
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_warning "Connector status: $status (timeout after ${max_wait}s)"
    return 1
}

get_all_connectors() {
    curl -sf "${CONNECT_URL}/connectors" 2>/dev/null | jq -r '.[]' 2>/dev/null
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
        -n|--name)
            CONNECTOR_NAME="$2"
            shift 2
            ;;
        -c|--connect)
            CONNECT_URL="$2"
            shift 2
            ;;
        --no-wait)
            WAIT_PAUSED=false
            shift
            ;;
        --all)
            ALL_CONNECTORS=true
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

log_info "==================================================================="
log_info "Kafka Connect Connector Pause"
log_info "==================================================================="
echo ""

# Check if Kafka Connect is accessible
if ! curl -sf "${CONNECT_URL}" &>/dev/null; then
    log_error "Kafka Connect is not accessible at ${CONNECT_URL}"
    exit 1
fi

# Pause all connectors
if [ "$ALL_CONNECTORS" = true ]; then
    log_info "Pausing all connectors..."
    echo ""

    connectors=$(get_all_connectors)
    if [ -z "$connectors" ]; then
        log_info "No connectors found"
        exit 0
    fi

    for connector in $connectors; do
        pause_connector "$connector" || log_warning "Failed to pause $connector"
        echo ""
    done

    log_success "All connectors paused"
    exit 0
fi

# Pause single connector
if [ -z "$CONNECTOR_NAME" ]; then
    log_error "Missing required option: --name"
    echo ""
    usage
    exit 1
fi

if pause_connector "$CONNECTOR_NAME"; then
    echo ""
    log_success "==================================================================="
    log_success "Connector Paused Successfully"
    log_success "==================================================================="
    echo ""
    log_info "To resume the connector:"
    echo "  ./scripts/resume-connector.sh --name $CONNECTOR_NAME"
    echo ""
    log_info "To check status:"
    echo "  curl -s ${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status | jq"
else
    echo ""
    log_error "Failed to pause connector"
    exit 1
fi
