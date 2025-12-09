#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Connector Deployment Script
# ==============================================================================
# Description: Deploy Kafka Connect connectors with Vault credential injection
# Usage: ./scripts/deploy-connector.sh [OPTIONS]
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
CONNECT_URL="http://localhost:8083"
VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-dev-token}"
CONNECTOR_NAME=""
CONNECTOR_TYPE=""
CONFIG_FILE=""
DRY_RUN=false
FORCE=false
WAIT_READY=true

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

Deploy Kafka Connect connectors with Vault credential injection.

OPTIONS:
    -h, --help              Show this help message
    -n, --name NAME         Connector name (scylla-source or postgres-sink)
    -t, --type TYPE         Connector type (source or sink)
    -f, --file FILE         Path to connector configuration JSON file
    -c, --connect URL       Kafka Connect REST API URL (default: $CONNECT_URL)
    -d, --dry-run           Validate configuration without deploying
    --force                 Force update if connector already exists
    --no-wait               Don't wait for connector to become ready
    --all                   Deploy all connectors (source and sink)

EXAMPLES:
    # Deploy source connector
    $0 --name scylla-source

    # Deploy sink connector
    $0 --name postgres-sink

    # Deploy all connectors
    $0 --all

    # Deploy custom connector configuration
    $0 --name my-connector --file ./custom-config.json

    # Dry run to validate configuration
    $0 --name scylla-source --dry-run

    # Force update existing connector
    $0 --name postgres-sink --force

NOTES:
    - Connectors must be defined in configs/connectors/ or docker/kafka-connect/connectors/
    - Vault must be running and accessible for credential injection
    - Kafka Connect must be healthy before deploying connectors
EOF
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check if jq is available
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed"
        log_info "Install with: apt-get install jq (Ubuntu) or brew install jq (Mac)"
        return 1
    fi

    # Check if curl is available
    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed"
        return 1
    fi

    # Check Kafka Connect is accessible
    if ! curl -sf "${CONNECT_URL}" &>/dev/null; then
        log_error "Kafka Connect is not accessible at ${CONNECT_URL}"
        log_info "Start services with: ./scripts/setup-local.sh"
        return 1
    fi

    # Check Vault is accessible
    if ! curl -sf "${VAULT_ADDR}/v1/sys/health" &>/dev/null; then
        log_warning "Vault is not accessible at ${VAULT_ADDR}"
        log_warning "Credential injection may fail if Vault is not available"
    fi

    log_success "Prerequisites check passed"
    return 0
}

get_connector_status() {
    local connector_name=$1
    local status

    status=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "NOT_FOUND")
    echo "$status"
}

connector_exists() {
    local connector_name=$1
    local exists

    exists=$(curl -sf "${CONNECT_URL}/connectors" 2>/dev/null | jq -r ".[] | select(. == \"${connector_name}\")" 2>/dev/null)

    if [ -n "$exists" ]; then
        return 0
    else
        return 1
    fi
}

find_connector_config() {
    local name=$1
    local locations=(
        "configs/connectors/${name}.json"
        "docker/kafka-connect/connectors/${name}.json"
        "${PROJECT_ROOT}/configs/connectors/${name}.json"
        "${PROJECT_ROOT}/docker/kafka-connect/connectors/${name}.json"
    )

    for location in "${locations[@]}"; do
        if [ -f "$location" ]; then
            echo "$location"
            return 0
        fi
    done

    return 1
}

validate_connector_config() {
    local config_file=$1

    log_info "Validating connector configuration..."

    if [ ! -f "$config_file" ]; then
        log_error "Configuration file not found: $config_file"
        return 1
    fi

    # Validate JSON syntax
    if ! jq empty "$config_file" 2>/dev/null; then
        log_error "Invalid JSON in configuration file"
        return 1
    fi

    # Check required fields
    local name
    local connector_class

    name=$(jq -r '.name' "$config_file" 2>/dev/null)
    connector_class=$(jq -r '.config."connector.class"' "$config_file" 2>/dev/null)

    if [ -z "$name" ] || [ "$name" = "null" ]; then
        log_error "Configuration missing required field: name"
        return 1
    fi

    if [ -z "$connector_class" ] || [ "$connector_class" = "null" ]; then
        log_error "Configuration missing required field: connector.class"
        return 1
    fi

    log_success "Configuration validation passed"
    return 0
}

deploy_connector() {
    local config_file=$1
    local connector_name
    local response
    local http_code

    connector_name=$(jq -r '.name' "$config_file")

    log_info "Deploying connector: $connector_name"

    # Check if connector already exists
    if connector_exists "$connector_name"; then
        if [ "$FORCE" = false ]; then
            log_warning "Connector $connector_name already exists"
            log_info "Use --force to update existing connector"
            log_info "Current status: $(get_connector_status "$connector_name")"
            return 1
        else
            log_warning "Connector exists, updating (--force specified)..."

            # Update existing connector
            response=$(curl -sf -X PUT \
                -H "Content-Type: application/json" \
                -d @"$config_file" \
                "${CONNECT_URL}/connectors/${connector_name}/config" 2>&1)

            if [ $? -eq 0 ]; then
                log_success "Connector $connector_name updated successfully"
            else
                log_error "Failed to update connector"
                echo "$response"
                return 1
            fi
        fi
    else
        # Create new connector
        response=$(curl -s -w "\n%{http_code}" -X POST \
            -H "Content-Type: application/json" \
            -d @"$config_file" \
            "${CONNECT_URL}/connectors")

        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')

        if [ "$http_code" = "201" ] || [ "$http_code" = "200" ]; then
            log_success "Connector $connector_name deployed successfully"
        else
            log_error "Failed to deploy connector (HTTP $http_code)"
            echo "$body" | jq '.' 2>/dev/null || echo "$body"
            return 1
        fi
    fi

    # Wait for connector to become ready
    if [ "$WAIT_READY" = true ]; then
        wait_for_connector "$connector_name"
    fi

    return 0
}

wait_for_connector() {
    local connector_name=$1
    local max_wait=60
    local elapsed=0
    local status

    log_info "Waiting for connector to become ready..."

    while [ $elapsed -lt $max_wait ]; do
        status=$(get_connector_status "$connector_name")

        if [ "$status" = "RUNNING" ]; then
            log_success "Connector $connector_name is running"

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
                return 0
            fi
        elif [ "$status" = "FAILED" ]; then
            log_error "Connector failed to start"
            show_connector_errors "$connector_name"
            return 1
        fi

        sleep 2
        elapsed=$((elapsed + 2))
        log_info "Status: $status (${elapsed}s elapsed)"
    done

    log_warning "Connector status: $status (timeout after ${max_wait}s)"
    log_info "Check status with: curl -s ${CONNECT_URL}/connectors/${connector_name}/status | jq"
    return 1
}

show_connector_errors() {
    local connector_name=$1
    local errors

    log_error "Connector errors:"
    errors=$(curl -sf "${CONNECT_URL}/connectors/${connector_name}/status" 2>/dev/null | jq -r '.tasks[]? | select(.state == "FAILED") | .trace' 2>/dev/null)

    if [ -n "$errors" ]; then
        echo "$errors"
    else
        log_info "No error details available"
    fi
}

show_connector_info() {
    local connector_name=$1

    log_info "Connector information:"
    echo "  Name: $connector_name"
    echo "  URL: ${CONNECT_URL}/connectors/${connector_name}"
    echo ""

    log_info "To check status:"
    echo "  curl -s ${CONNECT_URL}/connectors/${connector_name}/status | jq"
    echo ""

    log_info "To view configuration:"
    echo "  curl -s ${CONNECT_URL}/connectors/${connector_name}/config | jq"
    echo ""

    log_info "To pause connector:"
    echo "  ./scripts/pause-connector.sh --name $connector_name"
    echo ""

    log_info "To delete connector:"
    echo "  curl -X DELETE ${CONNECT_URL}/connectors/${connector_name}"
}

# ==============================================================================
# Argument Parsing
# ==============================================================================

DEPLOY_ALL=false

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
        -t|--type)
            CONNECTOR_TYPE="$2"
            shift 2
            ;;
        -f|--file)
            CONFIG_FILE="$2"
            shift 2
            ;;
        -c|--connect)
            CONNECT_URL="$2"
            shift 2
            ;;
        -d|--dry-run)
            DRY_RUN=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --no-wait)
            WAIT_READY=false
            shift
            ;;
        --all)
            DEPLOY_ALL=true
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
log_info "Kafka Connect Connector Deployment"
log_info "==================================================================="
echo ""

# Check prerequisites
if ! check_prerequisites; then
    exit 1
fi

echo ""

# Deploy all connectors if --all flag is set
if [ "$DEPLOY_ALL" = true ]; then
    log_info "Deploying all connectors..."
    echo ""

    # Deploy source connector
    log_info "Step 1/2: Deploying source connector..."
    if config_path=$(find_connector_config "scylla-source"); then
        if validate_connector_config "$config_path"; then
            if [ "$DRY_RUN" = false ]; then
                deploy_connector "$config_path" || log_warning "Source connector deployment had issues"
            else
                log_info "Dry run: would deploy $config_path"
            fi
        fi
    else
        log_error "Source connector configuration not found"
    fi

    echo ""

    # Deploy sink connector
    log_info "Step 2/2: Deploying sink connector..."
    if config_path=$(find_connector_config "postgres-sink"); then
        if validate_connector_config "$config_path"; then
            if [ "$DRY_RUN" = false ]; then
                deploy_connector "$config_path" || log_warning "Sink connector deployment had issues"
            else
                log_info "Dry run: would deploy $config_path"
            fi
        fi
    else
        log_error "Sink connector configuration not found"
    fi

    echo ""
    log_success "All connectors deployed!"
    echo ""
    log_info "Check status with: ./scripts/monitor-connectors.sh"
    exit 0
fi

# Single connector deployment
if [ -z "$CONNECTOR_NAME" ] && [ -z "$CONFIG_FILE" ]; then
    log_error "Missing required option: --name or --file"
    echo ""
    usage
    exit 1
fi

# Find connector configuration
if [ -n "$CONFIG_FILE" ]; then
    if [ ! -f "$CONFIG_FILE" ]; then
        log_error "Configuration file not found: $CONFIG_FILE"
        exit 1
    fi
else
    # Try to find config by name
    if ! CONFIG_FILE=$(find_connector_config "$CONNECTOR_NAME"); then
        log_error "Connector configuration not found: $CONNECTOR_NAME"
        log_info "Searched locations:"
        log_info "  - configs/connectors/${CONNECTOR_NAME}.json"
        log_info "  - docker/kafka-connect/connectors/${CONNECTOR_NAME}.json"
        exit 1
    fi
fi

log_info "Using configuration file: $CONFIG_FILE"
echo ""

# Validate configuration
if ! validate_connector_config "$CONFIG_FILE"; then
    exit 1
fi

echo ""

# Dry run mode
if [ "$DRY_RUN" = true ]; then
    log_info "Dry run mode - configuration is valid"
    log_info "Connector would be deployed to: $CONNECT_URL"
    echo ""
    log_info "Configuration:"
    jq '.' "$CONFIG_FILE"
    exit 0
fi

# Deploy connector
if deploy_connector "$CONFIG_FILE"; then
    echo ""
    log_success "==================================================================="
    log_success "Connector Deployment Complete!"
    log_success "==================================================================="
    echo ""

    connector_name=$(jq -r '.name' "$CONFIG_FILE")
    show_connector_info "$connector_name"
else
    echo ""
    log_error "Connector deployment failed"
    exit 1
fi
