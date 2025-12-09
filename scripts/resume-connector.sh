#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Resume Connector Script
# ==============================================================================
# Description: Resume a paused Kafka Connect connector
# Usage: ./scripts/resume-connector.sh [OPTIONS]
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
WAIT_RUNNING=true
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

Resume a paused Kafka Connect connector.

OPTIONS:
    -h, --help              Show this help message
    -n, --name NAME         Connector name to resume
    -c, --connect URL       Kafka Connect REST API URL (default: $CONNECT_URL)
    --no-wait               Don't wait for connector to start running
    --all                   Resume all paused connectors

EXAMPLES:
    $0 --name scylla-source
    $0 --name postgres-sink
    $0 --all

DESCRIPTION:
    Resumes a paused connector, allowing it to continue reading/writing data
    from where it left off using preserved offsets.
EOF
}

get_connector_status() {
    local connector_name=$1
    curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN"
}

resume_connector() {
    local connector_name=$1
    local current_status

    current_status=$(get_connector_status "$connector_name")

    if [ "$current_status" = "UNKNOWN" ]; then
        log_error "Connector not found: $connector_name"
        return 1
    fi

    if [ "$current_status" = "RUNNING" ]; then
        log_info "Connector $connector_name is already running"
        return 0
    fi

    log_info "Resuming connector: $connector_name (current status: $current_status)"

    if curl -sf -X PUT "${CONNECT_URL}/connectors/${connector_name}/resume" &>/dev/null; then
        log_success "Resume request sent successfully"

        if [ "$WAIT_RUNNING" = true ]; then
            wait_for_running "$connector_name"
        fi
    else
        log_error "Failed to resume connector"
        return 1
    fi
}

wait_for_running() {
    local connector_name=$1
    local max_wait=60
    local elapsed=0
    local status

    log_info "Waiting for connector to start running..."

    while [ $elapsed -lt $max_wait ]; do
        status=$(get_connector_status "$connector_name")

        if [ "$status" = "RUNNING" ]; then
            log_success "Connector $connector_name is now running"

            # Check task status
            local tasks_status
            tasks_status=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.tasks[].state' 2>/dev/null)

            local all_running=true
            for task_status in $tasks_status; do
                if [ "$task_status" != "RUNNING" ]; then
                    all_running=false
                    log_warning "Task status: $task_status"
                fi
            done

            if [ "$all_running" = true ]; then
                log_success "All tasks are running"
            fi

            return 0
        elif [ "$status" = "FAILED" ]; then
            log_error "Connector failed to start"
            return 1
        fi

        sleep 2
        elapsed=$((elapsed + 2))
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
            WAIT_RUNNING=false
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
log_info "Kafka Connect Connector Resume"
log_info "==================================================================="
echo ""

# Check if Kafka Connect is accessible
if ! curl -sf "${CONNECT_URL}" &>/dev/null; then
    log_error "Kafka Connect is not accessible at ${CONNECT_URL}"
    exit 1
fi

# Resume all connectors
if [ "$ALL_CONNECTORS" = true ]; then
    log_info "Resuming all connectors..."
    echo ""

    connectors=$(get_all_connectors)
    if [ -z "$connectors" ]; then
        log_info "No connectors found"
        exit 0
    fi

    for connector in $connectors; do
        resume_connector "$connector" || log_warning "Failed to resume $connector"
        echo ""
    done

    log_success "All connectors resumed"
    exit 0
fi

# Resume single connector
if [ -z "$CONNECTOR_NAME" ]; then
    log_error "Missing required option: --name"
    echo ""
    usage
    exit 1
fi

if resume_connector "$CONNECTOR_NAME"; then
    echo ""
    log_success "==================================================================="
    log_success "Connector Resumed Successfully"
    log_success "==================================================================="
    echo ""
    log_info "To pause the connector again:"
    echo "  ./scripts/pause-connector.sh --name $CONNECTOR_NAME"
    echo ""
    log_info "To check status:"
    echo "  curl -s ${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status | jq"
else
    echo ""
    log_error "Failed to resume connector"
    exit 1
fi
