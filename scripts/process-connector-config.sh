#!/usr/bin/env bash

# ============================================================================
# Connector Configuration Template Processor
# ============================================================================
# Processes connector configuration templates by substituting environment
# variables from the .env file. Creates the final JSON files used for
# deployment.
#
# Usage:
#   ./scripts/process-connector-config.sh [OPTIONS]
#
# Options:
#   --template FILE    Path to template file (required)
#   --output FILE      Path to output file (required)
#   --env-file FILE    Path to .env file (default: .env)
#   --validate         Validate JSON output
#   --help             Display this help message
#
# Example:
#   ./scripts/process-connector-config.sh \
#     --template docker/kafka-connect/connectors/scylla-source.json.template \
#     --output docker/kafka-connect/connectors/scylla-source.json \
#     --validate
# ============================================================================

set -euo pipefail

# Default values
TEMPLATE_FILE=""
OUTPUT_FILE=""
ENV_FILE=".env"
VALIDATE_JSON=false

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

load_env_file() {
    local env_file=$1

    if [ ! -f "$env_file" ]; then
        log_error "Environment file not found: $env_file"
        exit 1
    fi

    log_info "Loading environment variables from: $env_file"

    # Export variables from .env file (skip comments and empty lines)
    # Also remove inline comments and strip outer quotes from values
    set -a
    while IFS= read -r line; do
        # Skip empty lines and comment lines
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        # Remove inline comments (preserve # inside quotes)
        line=$(echo "$line" | sed 's/[[:space:]]*#.*$//')

        # Strip outer quotes from the value part (after =)
        if [[ "$line" =~ ^([^=]+)=(.+)$ ]]; then
            var_name="${BASH_REMATCH[1]}"
            var_value="${BASH_REMATCH[2]}"
            # Remove leading/trailing quotes if present
            var_value=$(echo "$var_value" | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
            line="${var_name}=${var_value}"
        fi

        # Export the variable
        export "$line"
    done < "$env_file"
    set +a

    log_success "Environment variables loaded"
}

substitute_variables() {
    local template_file=$1
    local output_file=$2

    if [ ! -f "$template_file" ]; then
        log_error "Template file not found: $template_file"
        exit 1
    fi

    log_info "Processing template: $template_file"

    # Extract all ${VAR} patterns from the template to build envsubst variable list
    # This prevents envsubst from replacing $Value, $1, etc. that are not in ${} format
    local var_list=$(grep -oE '\$\{[A-Z_][A-Z0-9_]*\}' "$template_file" | sort -u | tr '\n' ' ')

    if [ -n "$var_list" ]; then
        # Use envsubst with explicit variable list to avoid replacing $Value, $1, etc.
        envsubst "$var_list" < "$template_file" > "$output_file"
    else
        # No variables to substitute, just copy the file
        cp "$template_file" "$output_file"
    fi

    if [ $? -eq 0 ]; then
        log_success "Configuration generated: $output_file"
    else
        log_error "Failed to process template"
        exit 1
    fi
}

validate_json() {
    local json_file=$1

    log_info "Validating JSON output..."

    if command -v jq &> /dev/null; then
        if jq empty "$json_file" 2>/dev/null; then
            log_success "JSON is valid"
            return 0
        else
            log_error "Invalid JSON in output file"
            return 1
        fi
    else
        log_warn "jq not found, skipping JSON validation"
        return 0
    fi
}

# ============================================================================
# Parse Command Line Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --template)
            TEMPLATE_FILE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --env-file)
            ENV_FILE="$2"
            shift 2
            ;;
        --validate)
            VALIDATE_JSON=true
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
# Validate Arguments
# ============================================================================

if [ -z "$TEMPLATE_FILE" ]; then
    log_error "Template file not specified (use --template)"
    show_help
fi

if [ -z "$OUTPUT_FILE" ]; then
    log_error "Output file not specified (use --output)"
    show_help
fi

# ============================================================================
# Main Execution
# ============================================================================

log_info "Connector Configuration Template Processor"
log_info "=========================================="

# Load environment variables
load_env_file "$ENV_FILE"

# Create output directory if it doesn't exist
OUTPUT_DIR=$(dirname "$OUTPUT_FILE")
mkdir -p "$OUTPUT_DIR"

# Process template and substitute variables
substitute_variables "$TEMPLATE_FILE" "$OUTPUT_FILE"

# Validate JSON if requested
if [ "$VALIDATE_JSON" = true ]; then
    validate_json "$OUTPUT_FILE"
fi

log_success "Configuration processing complete"
exit 0
