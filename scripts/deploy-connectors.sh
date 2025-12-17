#!/usr/bin/env bash

# ============================================================================
# Kafka Connect Connector Deployment Script
# ============================================================================
# Deploys Scylla CDC source and Postgres JDBC sink connectors to Kafka Connect
#
# Usage:
#   ./scripts/deploy-connectors.sh [OPTIONS]
#
# Options:
#   --source-only     Deploy only the Scylla CDC source connector
#   --sink-only       Deploy only the Postgres JDBC sink connector
#   --delete          Delete existing connectors before deploying
#   --validate        Validate connector configurations without deploying
#   --status          Show status of deployed connectors
#   --help            Display this help message
#
# Environment Variables:
#   KAFKA_CONNECT_HOST    Kafka Connect host (default: localhost)
#   KAFKA_CONNECT_PORT    Kafka Connect port (default: 8083)
#   CONNECTOR_CONFIG_DIR  Directory containing connector configs (default: docker/kafka-connect/connectors)
# ============================================================================

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load environment variables from .env file
if [ -f "${PROJECT_ROOT}/.env" ]; then
    set -a
    source <(grep -v '^#' "${PROJECT_ROOT}/.env" | grep -v '^$' | sed 's/\r$//')
    set +a
else
    echo "Warning: .env file not found at ${PROJECT_ROOT}/.env"
fi

# Configuration
KAFKA_CONNECT_HOST="${KAFKA_CONNECT_HOST:-localhost}"
KAFKA_CONNECT_PORT="${KAFKA_CONNECT_PORT:-8083}"
KAFKA_CONNECT_URL="http://${KAFKA_CONNECT_HOST}:${KAFKA_CONNECT_PORT}"
CONNECTOR_CONFIG_DIR="${CONNECTOR_CONFIG_DIR:-docker/kafka-connect/connectors}"

# Connector names (loaded from .env or use defaults)
SOURCE_CONNECTOR_NAME="${SOURCE_CONNECTOR_NAME:-scylla-cdc-source}"
SINK_CONNECTOR_NAME="${SINK_CONNECTOR_NAME:-postgres-jdbc-sink}"

# Script options
DEPLOY_SOURCE=true
DEPLOY_SINK=true
DELETE_EXISTING=false
VALIDATE_ONLY=false
SHOW_STATUS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# ============================================================================
# Helper Functions
# ============================================================================

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

show_help() {
    sed -n '/^# ====/,/^# ====/p' "$0" | grep -v "^# ====" | sed 's/^# //'
    exit 0
}

wait_for_kafka_connect() {
    log_info "Waiting for Kafka Connect to be ready..."
    local retries=30
    local count=0

    while [ $count -lt $retries ]; do
        if curl -sf "${KAFKA_CONNECT_URL}/" > /dev/null 2>&1; then
            log_success "Kafka Connect is ready"
            return 0
        fi
        count=$((count + 1))
        echo -n "."
        sleep 2
    done

    log_error "Kafka Connect is not available at ${KAFKA_CONNECT_URL}"
    exit 1
}

check_scylla_schema() {
    log_info "Checking if Scylla schema is initialized..."

    # Check if app_data keyspace exists
    if docker exec scylla cqlsh -e "DESCRIBE KEYSPACE app_data;" &> /dev/null; then
        log_success "Scylla schema is initialized"
        return 0
    else
        log_warn "Scylla schema not found. Initializing..."

        # Initialize the schema
        if docker exec scylla cqlsh -f /docker-entrypoint-initdb.d/init.cql 2>&1 | grep -q "Warnings :"; then
            log_success "Scylla schema initialized successfully"
            return 0
        else
            log_error "Failed to initialize Scylla schema"
            return 1
        fi
    fi
}

process_connector_templates() {
    log_info "Processing connector configuration templates..."

    local template_processor="${SCRIPT_DIR}/process-connector-config.sh"

    if [ ! -f "$template_processor" ]; then
        log_error "Template processor not found: $template_processor"
        return 1
    fi

    # Process source connector template
    if [ -f "${CONNECTOR_CONFIG_DIR}/scylla-source.json.template" ]; then
        bash "$template_processor" \
            --template "${CONNECTOR_CONFIG_DIR}/scylla-source.json.template" \
            --output "${CONNECTOR_CONFIG_DIR}/scylla-source.json" \
            --env-file "${PROJECT_ROOT}/.env" \
            --validate || return 1
    else
        log_warn "Source connector template not found, using existing config"
    fi

    # Process sink connector template
    if [ -f "${CONNECTOR_CONFIG_DIR}/postgres-sink.json.template" ]; then
        bash "$template_processor" \
            --template "${CONNECTOR_CONFIG_DIR}/postgres-sink.json.template" \
            --output "${CONNECTOR_CONFIG_DIR}/postgres-sink.json" \
            --env-file "${PROJECT_ROOT}/.env" \
            --validate || return 1
    else
        log_warn "Sink connector template not found, using existing config"
    fi

    log_success "Connector configurations processed successfully"
}

check_connector_exists() {
    local connector_name=$1
    curl -sf "${KAFKA_CONNECT_URL}/connectors/${connector_name}" > /dev/null 2>&1
}

delete_connector() {
    local connector_name=$1

    if check_connector_exists "$connector_name"; then
        log_info "Deleting existing connector: ${connector_name}"
        if curl -sf -X DELETE "${KAFKA_CONNECT_URL}/connectors/${connector_name}" > /dev/null 2>&1; then
            log_success "Connector deleted: ${connector_name}"
            sleep 2
        else
            log_error "Failed to delete connector: ${connector_name}"
            return 1
        fi
    else
        log_info "Connector does not exist: ${connector_name}"
    fi
}

validate_connector_config() {
    local config_file=$1
    local connector_class

    connector_class=$(jq -r '.config["connector.class"]' "$config_file")

    log_info "Validating connector configuration: ${config_file}"

    response=$(curl -sf -X PUT \
        -H "Content-Type: application/json" \
        --data @"${config_file}" \
        "${KAFKA_CONNECT_URL}/connector-plugins/${connector_class}/config/validate" 2>&1)

    if [ $? -eq 0 ]; then
        errors=$(echo "$response" | jq -r '.configs[] | select(.value.errors | length > 0) | .value.errors[]' 2>/dev/null)

        if [ -z "$errors" ]; then
            log_success "Configuration is valid"
            return 0
        else
            log_error "Configuration validation errors:"
            echo "$errors"
            return 1
        fi
    else
        log_error "Failed to validate configuration"
        return 1
    fi
}

deploy_connector() {
    local config_file=$1
    local connector_name

    connector_name=$(jq -r '.name' "$config_file")

    log_info "Deploying connector: ${connector_name}"

    response=$(curl -sf -X POST \
        -H "Content-Type: application/json" \
        --data @"${config_file}" \
        "${KAFKA_CONNECT_URL}/connectors" 2>&1)

    if [ $? -eq 0 ]; then
        log_success "Connector deployed successfully: ${connector_name}"
        show_connector_status "$connector_name"
        return 0
    else
        log_error "Failed to deploy connector: ${connector_name}"
        echo "$response" | jq '.' 2>/dev/null || echo "$response"
        return 1
    fi
}

update_connector() {
    local config_file=$1
    local connector_name

    connector_name=$(jq -r '.name' "$config_file")

    log_info "Updating connector configuration: ${connector_name}"

    response=$(curl -sf -X PUT \
        -H "Content-Type: application/json" \
        --data "$(jq -c '.config' "$config_file")" \
        "${KAFKA_CONNECT_URL}/connectors/${connector_name}/config" 2>&1)

    if [ $? -eq 0 ]; then
        log_success "Connector updated successfully: ${connector_name}"
        show_connector_status "$connector_name"
        return 0
    else
        log_error "Failed to update connector: ${connector_name}"
        echo "$response" | jq '.' 2>/dev/null || echo "$response"
        return 1
    fi
}

show_connector_status() {
    local connector_name=$1

    log_info "Fetching status for connector: ${connector_name}"

    # Wait a moment for connector to fully initialize
    sleep 2

    response=$(curl -sf "${KAFKA_CONNECT_URL}/connectors/${connector_name}/status" 2>&1)

    if [ $? -eq 0 ]; then
        echo "$response" | jq '{
            name: .name,
            state: .connector.state,
            worker_id: .connector.worker_id,
            tasks: [.tasks[] | {id: .id, state: .state, worker_id: .worker_id}]
        }'
    else
        log_error "Failed to fetch connector status"
        return 1
    fi
}

list_all_connectors() {
    log_info "Fetching all deployed connectors..."

    response=$(curl -sf "${KAFKA_CONNECT_URL}/connectors?expand=status" 2>&1)

    if [ $? -eq 0 ]; then
        echo "$response" | jq 'to_entries[] | {
            name: .key,
            state: .value.status.connector.state,
            tasks_running: [.value.status.tasks[] | select(.state == "RUNNING")] | length,
            tasks_failed: [.value.status.tasks[] | select(.state == "FAILED")] | length
        }'
    else
        log_error "Failed to fetch connectors"
        return 1
    fi
}

# ============================================================================
# Parse Command Line Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --source-only)
            DEPLOY_SOURCE=true
            DEPLOY_SINK=false
            shift
            ;;
        --sink-only)
            DEPLOY_SOURCE=false
            DEPLOY_SINK=true
            shift
            ;;
        --delete)
            DELETE_EXISTING=true
            shift
            ;;
        --validate)
            VALIDATE_ONLY=true
            shift
            ;;
        --status)
            SHOW_STATUS=true
            shift
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

log_info "Kafka Connect URL: ${KAFKA_CONNECT_URL}"
log_info "Connector Config Directory: ${CONNECTOR_CONFIG_DIR}"

# Wait for Kafka Connect to be available
wait_for_kafka_connect

# Show status and exit if requested
if [ "$SHOW_STATUS" = true ]; then
    list_all_connectors
    exit 0
fi

# Process connector configuration templates
process_connector_templates || {
    log_error "Failed to process connector configuration templates"
    exit 1
}

# Validate configurations
if [ "$VALIDATE_ONLY" = true ]; then
    log_info "Validation mode: configurations will not be deployed"

    if [ "$DEPLOY_SOURCE" = true ]; then
        validate_connector_config "${CONNECTOR_CONFIG_DIR}/scylla-source.json"
    fi

    if [ "$DEPLOY_SINK" = true ]; then
        validate_connector_config "${CONNECTOR_CONFIG_DIR}/postgres-sink.json"
    fi

    exit 0
fi

# Delete existing connectors if requested
if [ "$DELETE_EXISTING" = true ]; then
    if [ "$DEPLOY_SOURCE" = true ]; then
        delete_connector "$SOURCE_CONNECTOR_NAME"
    fi

    if [ "$DEPLOY_SINK" = true ]; then
        delete_connector "$SINK_CONNECTOR_NAME"
    fi
fi

# Deploy connectors
EXIT_CODE=0

if [ "$DEPLOY_SOURCE" = true ]; then
    # Check and initialize Scylla schema if needed
    check_scylla_schema || EXIT_CODE=1

    log_info "Processing Scylla CDC source connector..."

    if check_connector_exists "$SOURCE_CONNECTOR_NAME"; then
        log_warn "Connector already exists, updating configuration..."
        update_connector "${CONNECTOR_CONFIG_DIR}/scylla-source.json" || EXIT_CODE=1
    else
        deploy_connector "${CONNECTOR_CONFIG_DIR}/scylla-source.json" || EXIT_CODE=1
    fi
fi

if [ "$DEPLOY_SINK" = true ]; then
    log_info "Processing Postgres JDBC sink connector..."

    if check_connector_exists "$SINK_CONNECTOR_NAME"; then
        log_warn "Connector already exists, updating configuration..."
        update_connector "${CONNECTOR_CONFIG_DIR}/postgres-sink.json" || EXIT_CODE=1
    else
        deploy_connector "${CONNECTOR_CONFIG_DIR}/postgres-sink.json" || EXIT_CODE=1
    fi
fi

# Summary
echo ""
log_info "Deployment Summary:"
list_all_connectors

if [ $EXIT_CODE -eq 0 ]; then
    log_success "All connectors deployed successfully"
else
    log_error "Some connectors failed to deploy"
fi

exit $EXIT_CODE
