#!/usr/bin/env bash

# ============================================================================
# Kafka Connect Connector Monitoring Script
# ============================================================================
# Monitors health and performance of Kafka Connect connectors
#
# Usage:
#   ./scripts/monitor-connectors.sh [OPTIONS]
#
# Options:
#   --watch SECONDS       Continuously monitor with refresh interval (default: 5)
#   --json                Output in JSON format
#   --metrics             Show detailed metrics
#   --logs CONNECTOR      Show recent logs for a specific connector
#   --restart CONNECTOR   Restart a specific connector
#   --pause CONNECTOR     Pause a specific connector
#   --resume CONNECTOR    Resume a paused connector
#   --help                Display this help message
#
# Environment Variables:
#   KAFKA_CONNECT_HOST    Kafka Connect host (default: localhost)
#   KAFKA_CONNECT_PORT    Kafka Connect port (default: 8083)
# ============================================================================

set -euo pipefail

# Configuration
KAFKA_CONNECT_HOST="${KAFKA_CONNECT_HOST:-localhost}"
KAFKA_CONNECT_PORT="${KAFKA_CONNECT_PORT:-8083}"
KAFKA_CONNECT_URL="http://${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}"

# Script options
WATCH_MODE=false
WATCH_INTERVAL=5
JSON_OUTPUT=false
SHOW_METRICS=false
SHOW_LOGS=""
RESTART_CONNECTOR=""
PAUSE_CONNECTOR=""
RESUME_CONNECTOR=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    if [ "$JSON_OUTPUT" = false ]; then
        echo -e "${BLUE}[INFO]${NC} $1"
    fi
}

log_success() {
    if [ "$JSON_OUTPUT" = false ]; then
        echo -e "${GREEN}[SUCCESS]${NC} $1"
    fi
}

log_warn() {
    if [ "$JSON_OUTPUT" = false ]; then
        echo -e "${YELLOW}[WARN]${NC} $1"
    fi
}

log_error() {
    if [ "$JSON_OUTPUT" = false ]; then
        echo -e "${RED}[ERROR]${NC} $1"
    fi
}

show_help() {
    sed -n '/^# ====/,/^# ====/p' "$0" | grep -v "^# ====" | sed 's/^# //'
    exit 0
}

check_kafka_connect() {
    if ! curl -sf "${KAFKA_CONNECT_URL}/" > /dev/null 2>&1; then
        log_error "Kafka Connect is not available at ${KAFKA_CONNECT_URL}"
        exit 1
    fi
}

get_connector_status() {
    local connector_name=$1
    curl -sf "${KAFKA_CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null
}

get_all_connectors() {
    curl -sf "${KAFKA_CONNECT_URL}/connectors?expand=status" 2>/dev/null
}

format_state() {
    local state=$1

    case "$state" in
        RUNNING)
            echo -e "${GREEN}${state}${NC}"
            ;;
        FAILED)
            echo -e "${RED}${state}${NC}"
            ;;
        PAUSED)
            echo -e "${YELLOW}${state}${NC}"
            ;;
        *)
            echo -e "${CYAN}${state}${NC}"
            ;;
    esac
}

show_connector_summary() {
    local connector_data=$1
    local connector_name
    local state
    local worker_id
    local tasks_count
    local tasks_running
    local tasks_failed

    connector_name=$(echo "$connector_data" | jq -r '.name')
    state=$(echo "$connector_data" | jq -r '.connector.state')
    worker_id=$(echo "$connector_data" | jq -r '.connector.worker_id')
    tasks_count=$(echo "$connector_data" | jq '.tasks | length')
    tasks_running=$(echo "$connector_data" | jq '[.tasks[] | select(.state == "RUNNING")] | length')
    tasks_failed=$(echo "$connector_data" | jq '[.tasks[] | select(.state == "FAILED")] | length')

    if [ "$JSON_OUTPUT" = true ]; then
        echo "$connector_data" | jq '{
            name: .name,
            state: .connector.state,
            worker_id: .connector.worker_id,
            tasks: {
                total: (.tasks | length),
                running: ([.tasks[] | select(.state == "RUNNING")] | length),
                failed: ([.tasks[] | select(.state == "FAILED")] | length)
            }
        }'
    else
        echo -e "${BOLD}${connector_name}${NC}"
        echo -e "  State:    $(format_state "$state")"
        echo -e "  Worker:   ${worker_id}"
        echo -e "  Tasks:    ${tasks_running}/${tasks_count} running"
        if [ "$tasks_failed" -gt 0 ]; then
            echo -e "  ${RED}Failed tasks: ${tasks_failed}${NC}"
        fi

        # Show task details
        echo "$connector_data" | jq -r '.tasks[] | "  Task \(.id): \(.state) on \(.worker_id)"' | while read -r line; do
            if [[ $line == *"FAILED"* ]]; then
                echo -e "${RED}${line}${NC}"
            elif [[ $line == *"RUNNING"* ]]; then
                echo -e "${GREEN}${line}${NC}"
            else
                echo -e "${CYAN}${line}${NC}"
            fi
        done

        # Show error trace if failed
        if [ "$state" = "FAILED" ]; then
            error_trace=$(echo "$connector_data" | jq -r '.connector.trace // empty')
            if [ -n "$error_trace" ]; then
                echo -e "  ${RED}Error: ${error_trace}${NC}"
            fi
        fi

        echo ""
    fi
}

monitor_all_connectors() {
    local connectors_data

    connectors_data=$(get_all_connectors)

    if [ -z "$connectors_data" ]; then
        log_error "Failed to fetch connector data"
        return 1
    fi

    if [ "$JSON_OUTPUT" = true ]; then
        echo "$connectors_data" | jq 'to_entries[] | .value.status'
    else
        clear
        echo -e "${BOLD}${CYAN}=== Kafka Connect Connector Monitor ===${NC}"
        echo -e "Time: $(date '+%Y-%m-%d %H:%M:%S')"
        echo -e "Kafka Connect: ${KAFKA_CONNECT_URL}"
        echo ""

        local connector_count
        connector_count=$(echo "$connectors_data" | jq 'keys | length')

        if [ "$connector_count" -eq 0 ]; then
            log_warn "No connectors deployed"
            return 0
        fi

        echo "$connectors_data" | jq -c 'to_entries[] | .value.status' | while read -r connector_status; do
            show_connector_summary "$connector_status"
        done
    fi
}

show_connector_metrics() {
    local connector_name=$1
    local metrics_url="${KAFKA_CONNECT_URL}/connectors/${connector_name}/topics"

    log_info "Fetching metrics for connector: ${connector_name}"

    topics=$(curl -sf "$metrics_url" 2>/dev/null)

    if [ $? -eq 0 ]; then
        echo "Topics:"
        echo "$topics" | jq -r '.topics[]' | while read -r topic; do
            echo "  - $topic"
        done
    else
        log_error "Failed to fetch metrics"
        return 1
    fi
}

restart_connector() {
    local connector_name=$1

    log_info "Restarting connector: ${connector_name}"

    response=$(curl -sf -X POST "${KAFKA_CONNECT_URL}/connectors/${connector_name}/restart" 2>&1)

    if [ $? -eq 0 ]; then
        log_success "Connector restarted: ${connector_name}"
        sleep 2
        get_connector_status "$connector_name" | jq '.'
    else
        log_error "Failed to restart connector"
        echo "$response"
        return 1
    fi
}

pause_connector() {
    local connector_name=$1

    log_info "Pausing connector: ${connector_name}"

    response=$(curl -sf -X PUT "${KAFKA_CONNECT_URL}/connectors/${connector_name}/pause" 2>&1)

    if [ $? -eq 0 ]; then
        log_success "Connector paused: ${connector_name}"
        sleep 1
        get_connector_status "$connector_name" | jq '.'
    else
        log_error "Failed to pause connector"
        echo "$response"
        return 1
    fi
}

resume_connector() {
    local connector_name=$1

    log_info "Resuming connector: ${connector_name}"

    response=$(curl -sf -X PUT "${KAFKA_CONNECT_URL}/connectors/${connector_name}/resume" 2>&1)

    if [ $? -eq 0 ]; then
        log_success "Connector resumed: ${connector_name}"
        sleep 1
        get_connector_status "$connector_name" | jq '.'
    else
        log_error "Failed to resume connector"
        echo "$response"
        return 1
    fi
}

show_connector_logs() {
    local connector_name=$1

    log_info "Fetching recent logs for connector: ${connector_name} (from Docker)"

    docker logs --tail 100 kafka-connect 2>&1 | grep -i "$connector_name" || echo "No recent logs found"
}

# ============================================================================
# Parse Command Line Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --watch)
            WATCH_MODE=true
            if [[ $# -gt 1 && $2 =~ ^[0-9]+$ ]]; then
                WATCH_INTERVAL=$2
                shift
            fi
            shift
            ;;
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --metrics)
            SHOW_METRICS=true
            shift
            ;;
        --logs)
            SHOW_LOGS=$2
            shift 2
            ;;
        --restart)
            RESTART_CONNECTOR=$2
            shift 2
            ;;
        --pause)
            PAUSE_CONNECTOR=$2
            shift 2
            ;;
        --resume)
            RESUME_CONNECTOR=$2
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            show_help
            ;;
    esac
done

# ============================================================================
# Main Execution
# ============================================================================

check_kafka_connect

# Handle specific actions
if [ -n "$RESTART_CONNECTOR" ]; then
    restart_connector "$RESTART_CONNECTOR"
    exit $?
fi

if [ -n "$PAUSE_CONNECTOR" ]; then
    pause_connector "$PAUSE_CONNECTOR"
    exit $?
fi

if [ -n "$RESUME_CONNECTOR" ]; then
    resume_connector "$RESUME_CONNECTOR"
    exit $?
fi

if [ -n "$SHOW_LOGS" ]; then
    show_connector_logs "$SHOW_LOGS"
    exit $?
fi

# Monitor mode
if [ "$WATCH_MODE" = true ]; then
    log_info "Starting continuous monitoring (refresh every ${WATCH_INTERVAL}s, press Ctrl+C to exit)"
    while true; do
        monitor_all_connectors
        sleep "$WATCH_INTERVAL"
    done
else
    monitor_all_connectors
fi
