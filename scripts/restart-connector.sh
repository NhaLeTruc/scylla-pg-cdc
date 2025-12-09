#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Restart Connector Script
# ==============================================================================
# Description: Restart a Kafka Connect connector or specific tasks
# Usage: ./scripts/restart-connector.sh [OPTIONS]
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
TASK_ID=""
WAIT_RUNNING=true
ALL_CONNECTORS=false
INCLUDE_TASKS=false

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

Restart a Kafka Connect connector or specific task.

OPTIONS:
    -h, --help              Show this help message
    -n, --name NAME         Connector name to restart
    -t, --task ID           Task ID to restart (0-based index)
    -c, --connect URL       Kafka Connect REST API URL (default: $CONNECT_URL)
    --no-wait               Don't wait for connector to start running
    --all                   Restart all connectors
    --include-tasks         Also restart all tasks when restarting connector

EXAMPLES:
    # Restart connector
    $0 --name scylla-source

    # Restart connector and all its tasks
    $0 --name scylla-source --include-tasks

    # Restart specific task
    $0 --name scylla-source --task 0

    # Restart all connectors
    $0 --all

DESCRIPTION:
    Restarts a connector or task. This stops and starts the connector/task,
    which can help recover from transient errors or apply configuration changes.
EOF
}

get_connector_status() {
    local connector_name=$1
    curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN"
}

restart_connector() {
    local connector_name=$1

    if [ "$(get_connector_status "$connector_name")" = "UNKNOWN" ]; then
        log_error "Connector not found: $connector_name"
        return 1
    fi

    log_info "Restarting connector: $connector_name"

    if curl -sf -X POST "${CONNECT_URL}/connectors/${connector_name}/restart" &>/dev/null; then
        log_success "Connector restart initiated"

        if [ "$INCLUDE_TASKS" = true ]; then
            restart_all_tasks "$connector_name"
        fi

        if [ "$WAIT_RUNNING" = true ]; then
            wait_for_running "$connector_name"
        fi

        return 0
    else
        log_error "Failed to restart connector"
        return 1
    fi
}

restart_task() {
    local connector_name=$1
    local task_id=$2

    log_info "Restarting task: $connector_name (task $task_id)"

    if curl -sf -X POST "${CONNECT_URL}/connectors/${connector_name}/tasks/${task_id}/restart" &>/dev/null; then
        log_success "Task restart initiated"

        if [ "$WAIT_RUNNING" = true ]; then
            sleep 2
            wait_for_task_running "$connector_name" "$task_id"
        fi

        return 0
    else
        log_error "Failed to restart task"
        return 1
    fi
}

restart_all_tasks() {
    local connector_name=$1
    local tasks_status
    local task_count

    tasks_status=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null)
    task_count=$(echo "$tasks_status" | jq '.tasks | length' 2>/dev/null || echo "0")

    if [ "$task_count" = "0" ]; then
        log_info "No tasks to restart"
        return 0
    fi

    log_info "Restarting $task_count task(s)..."

    for ((i=0; i<task_count; i++)); do
        log_info "Restarting task $i..."
        if curl -sf -X POST "${CONNECT_URL}/connectors/${connector_name}/tasks/${i}/restart" &>/dev/null; then
            log_success "Task $i restart initiated"
        else
            log_warning "Failed to restart task $i"
        fi
    done
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
            log_success "Connector $connector_name is running"

            # Check all tasks are running
            local failed_tasks
            failed_tasks=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | \
                jq -r '.tasks[] | select(.state != "RUNNING") | .id' 2>/dev/null)

            if [ -z "$failed_tasks" ]; then
                log_success "All tasks are running"
                return 0
            else
                log_warning "Some tasks are not running: $failed_tasks"
            fi
        elif [ "$status" = "FAILED" ]; then
            log_error "Connector failed to start"
            show_connector_errors "$connector_name"
            return 1
        fi

        sleep 2
        elapsed=$((elapsed + 2))
    done

    log_warning "Connector status: $status (timeout after ${max_wait}s)"
    return 1
}

wait_for_task_running() {
    local connector_name=$1
    local task_id=$2
    local max_wait=30
    local elapsed=0
    local task_state

    log_info "Waiting for task to start running..."

    while [ $elapsed -lt $max_wait ]; do
        task_state=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | \
            jq -r ".tasks[] | select(.id == $task_id) | .state" 2>/dev/null || echo "UNKNOWN")

        if [ "$task_state" = "RUNNING" ]; then
            log_success "Task $task_id is running"
            return 0
        elif [ "$task_state" = "FAILED" ]; then
            log_error "Task failed to start"
            return 1
        fi

        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_warning "Task state: $task_state (timeout after ${max_wait}s)"
    return 1
}

show_connector_errors() {
    local connector_name=$1
    local errors

    log_error "Connector errors:"
    errors=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | \
        jq -r '.tasks[]? | select(.state == "FAILED") | .trace' 2>/dev/null)

    if [ -n "$errors" ]; then
        echo "$errors"
    else
        log_info "No error details available"
    fi
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
        -t|--task)
            TASK_ID="$2"
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
        --include-tasks)
            INCLUDE_TASKS=true
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
log_info "Kafka Connect Connector Restart"
log_info "==================================================================="
echo ""

# Check if Kafka Connect is accessible
if ! curl -sf "${CONNECT_URL}" &>/dev/null; then
    log_error "Kafka Connect is not accessible at ${CONNECT_URL}"
    exit 1
fi

# Restart all connectors
if [ "$ALL_CONNECTORS" = true ]; then
    log_info "Restarting all connectors..."
    echo ""

    connectors=$(get_all_connectors)
    if [ -z "$connectors" ]; then
        log_info "No connectors found"
        exit 0
    fi

    for connector in $connectors; do
        restart_connector "$connector" || log_warning "Failed to restart $connector"
        echo ""
    done

    log_success "All connectors restarted"
    exit 0
fi

# Validate input
if [ -z "$CONNECTOR_NAME" ]; then
    log_error "Missing required option: --name"
    echo ""
    usage
    exit 1
fi

# Restart specific task
if [ -n "$TASK_ID" ]; then
    if restart_task "$CONNECTOR_NAME" "$TASK_ID"; then
        echo ""
        log_success "==================================================================="
        log_success "Task Restarted Successfully"
        log_success "==================================================================="
    else
        echo ""
        log_error "Failed to restart task"
        exit 1
    fi
    exit 0
fi

# Restart connector
if restart_connector "$CONNECTOR_NAME"; then
    echo ""
    log_success "==================================================================="
    log_success "Connector Restarted Successfully"
    log_success "==================================================================="
    echo ""
    log_info "To check status:"
    echo "  curl -s ${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status | jq"
else
    echo ""
    log_error "Failed to restart connector"
    exit 1
fi
