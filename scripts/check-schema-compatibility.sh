#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Schema Compatibility Checker
# ==============================================================================
# Description: Validate schema changes before applying them
# Usage: ./scripts/check-schema-compatibility.sh [OPTIONS]
# ==============================================================================

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SUBJECT=""
NEW_SCHEMA_FILE=""
COMPATIBILITY_MODE="BACKWARD"
VERBOSE=false

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

Check if a schema change is compatible with existing schemas in Schema Registry.

OPTIONS:
    -h, --help              Show this help message
    -s, --subject SUBJECT   Subject name (e.g., cdc.scylla.users-value)
    -f, --file FILE         Path to new schema JSON file
    -u, --url URL           Schema Registry URL (default: $SCHEMA_REGISTRY_URL)
    -m, --mode MODE         Compatibility mode (BACKWARD, FORWARD, FULL, NONE)
    -v, --verbose           Enable verbose output
    --list-subjects         List all subjects in registry
    --get-schema            Get current schema for subject
    --get-versions          Get all versions for subject

EXAMPLES:
    # Check compatibility of new schema
    $0 --subject cdc.scylla.users-value --file new-schema.json

    # List all subjects
    $0 --list-subjects

    # Get current schema for subject
    $0 --subject cdc.scylla.users-value --get-schema

    # Check with specific compatibility mode
    $0 --subject cdc.scylla.users-value --file new-schema.json --mode FULL

DESCRIPTION:
    This script validates schema changes against the Schema Registry before
    applying them. It checks compatibility according to the configured mode
    and provides detailed information about what changed.
EOF
}

check_prerequisites() {
    if ! command -v curl &> /dev/null; then
        log_error "curl is required but not installed"
        return 1
    fi

    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed"
        return 1
    fi

    # Check if Schema Registry is accessible
    if ! curl -sf "${SCHEMA_REGISTRY_URL}" &>/dev/null; then
        log_error "Schema Registry is not accessible at ${SCHEMA_REGISTRY_URL}"
        return 1
    fi

    log_verbose "Prerequisites check passed"
    return 0
}

list_all_subjects() {
    log_info "Listing all subjects in Schema Registry..."
    echo ""

    local subjects
    subjects=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects" | jq -r '.[]')

    if [ -z "$subjects" ]; then
        log_info "No subjects found"
        return 0
    fi

    echo "Subjects:"
    for subject in $subjects; do
        local versions
        versions=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" | jq -r '. | length')
        echo "  - $subject ($versions version(s))"
    done
}

get_current_schema() {
    local subject=$1

    log_info "Getting current schema for: $subject"
    echo ""

    local response
    response=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest" 2>&1)

    if [ $? -ne 0 ]; then
        log_error "Subject not found: $subject"
        return 1
    fi

    local version
    local schema
    version=$(echo "$response" | jq -r '.version')
    schema=$(echo "$response" | jq -r '.schema' | jq '.')

    echo "Subject: $subject"
    echo "Version: $version"
    echo ""
    echo "Schema:"
    echo "$schema" | jq '.'
}

get_all_versions() {
    local subject=$1

    log_info "Getting all versions for: $subject"
    echo ""

    local versions
    versions=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" 2>&1)

    if [ $? -ne 0 ]; then
        log_error "Subject not found: $subject"
        return 1
    fi

    local version_list
    version_list=$(echo "$versions" | jq -r '.[]')

    echo "Versions for $subject:"
    for version in $version_list; do
        local schema_resp
        schema_resp=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/${version}")

        local schema_id
        schema_id=$(echo "$schema_resp" | jq -r '.id')

        echo "  Version $version (ID: $schema_id)"

        if [ "$VERBOSE" = true ]; then
            local schema
            schema=$(echo "$schema_resp" | jq -r '.schema' | jq -c '.')
            echo "    Schema: $schema"
        fi
    done
}

check_compatibility() {
    local subject=$1
    local schema_file=$2

    log_info "Checking schema compatibility..."
    echo ""

    # Validate schema file exists
    if [ ! -f "$schema_file" ]; then
        log_error "Schema file not found: $schema_file"
        return 1
    fi

    # Read and validate JSON
    local new_schema
    if ! new_schema=$(jq -c '.' "$schema_file" 2>&1); then
        log_error "Invalid JSON in schema file"
        echo "$new_schema"
        return 1
    fi

    log_verbose "Schema file loaded successfully"

    # Get current schema
    log_info "Fetching current schema from registry..."
    local current_response
    current_response=$(curl -sf "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest" 2>&1)

    if [ $? -ne 0 ]; then
        log_warning "Subject not found in registry (this would be the first version)"
        log_success "New schema can be registered"
        return 0
    fi

    local current_version
    local current_schema
    current_version=$(echo "$current_response" | jq -r '.version')
    current_schema=$(echo "$current_response" | jq -r '.schema')

    log_verbose "Current version: $current_version"

    # Check compatibility via API
    log_info "Testing compatibility with Schema Registry..."
    local compat_payload
    compat_payload=$(jq -n --arg schema "$new_schema" '{"schema": $schema}')

    local compat_response
    local http_code
    compat_response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "$compat_payload" \
        "${SCHEMA_REGISTRY_URL}/compatibility/subjects/${subject}/versions/latest")

    http_code=$(echo "$compat_response" | tail -n1)
    local body
    body=$(echo "$compat_response" | sed '$d')

    if [ "$http_code" != "200" ]; then
        log_error "Compatibility check failed (HTTP $http_code)"
        echo "$body" | jq '.' 2>/dev/null || echo "$body"
        return 1
    fi

    local is_compatible
    is_compatible=$(echo "$body" | jq -r '.is_compatible')

    echo ""
    if [ "$is_compatible" = "true" ]; then
        log_success "Schema is COMPATIBLE ✓"
        echo ""
        log_info "Schema changes can be safely applied"

        # Show differences if verbose
        if [ "$VERBOSE" = true ]; then
            echo ""
            log_info "Analyzing changes..."

            local current_fields
            local new_fields
            current_fields=$(echo "$current_schema" | jq -r '.fields[]?.name' 2>/dev/null | sort)
            new_fields=$(echo "$new_schema" | jq -r '.fields[]?.name' 2>/dev/null | sort)

            # Added fields
            local added
            added=$(comm -13 <(echo "$current_fields") <(echo "$new_fields"))
            if [ -n "$added" ]; then
                echo ""
                log_info "Added fields:"
                for field in $added; do
                    local field_type
                    field_type=$(echo "$new_schema" | jq -r ".fields[] | select(.name == \"$field\") | .type")
                    echo "  + $field ($field_type)"
                done
            fi

            # Removed fields
            local removed
            removed=$(comm -23 <(echo "$current_fields") <(echo "$new_fields"))
            if [ -n "$removed" ]; then
                echo ""
                log_warning "Removed fields:"
                for field in $removed; do
                    echo "  - $field"
                done
            fi

            # If no changes
            if [ -z "$added" ] && [ -z "$removed" ]; then
                echo ""
                log_info "No field changes detected"
            fi
        fi

        return 0
    else
        log_error "Schema is INCOMPATIBLE ✗"
        echo ""
        log_error "Schema changes would break compatibility"

        # Try to show why it's incompatible
        if [ "$VERBOSE" = true ]; then
            echo ""
            log_info "Analyzing incompatibility..."

            # Show messages if available
            local messages
            messages=$(echo "$body" | jq -r '.messages[]?' 2>/dev/null)
            if [ -n "$messages" ]; then
                echo "Reasons:"
                echo "$messages"
            fi
        fi

        return 1
    fi
}

get_compatibility_mode() {
    local subject=$1

    log_info "Getting compatibility mode for: $subject"

    local mode
    mode=$(curl -sf "${SCHEMA_REGISTRY_URL}/config/${subject}" 2>&1 | jq -r '.compatibilityLevel' 2>/dev/null)

    if [ "$mode" = "null" ] || [ -z "$mode" ]; then
        # Try global config
        mode=$(curl -sf "${SCHEMA_REGISTRY_URL}/config" | jq -r '.compatibilityLevel' 2>/dev/null)
        echo "Compatibility mode (global): $mode"
    else
        echo "Compatibility mode (subject): $mode"
    fi
}

# ==============================================================================
# Argument Parsing
# ==============================================================================

LIST_SUBJECTS=false
GET_SCHEMA=false
GET_VERSIONS=false
GET_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -s|--subject)
            SUBJECT="$2"
            shift 2
            ;;
        -f|--file)
            NEW_SCHEMA_FILE="$2"
            shift 2
            ;;
        -u|--url)
            SCHEMA_REGISTRY_URL="$2"
            shift 2
            ;;
        -m|--mode)
            COMPATIBILITY_MODE="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --list-subjects)
            LIST_SUBJECTS=true
            shift
            ;;
        --get-schema)
            GET_SCHEMA=true
            shift
            ;;
        --get-versions)
            GET_VERSIONS=true
            shift
            ;;
        --get-mode)
            GET_MODE=true
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
log_info "Schema Compatibility Checker"
log_info "==================================================================="
echo ""

# Check prerequisites
if ! check_prerequisites; then
    exit 1
fi

log_verbose "Schema Registry URL: $SCHEMA_REGISTRY_URL"
echo ""

# List subjects mode
if [ "$LIST_SUBJECTS" = true ]; then
    list_all_subjects
    exit 0
fi

# Get schema mode
if [ "$GET_SCHEMA" = true ]; then
    if [ -z "$SUBJECT" ]; then
        log_error "Subject required for --get-schema"
        exit 1
    fi
    get_current_schema "$SUBJECT"
    exit 0
fi

# Get versions mode
if [ "$GET_VERSIONS" = true ]; then
    if [ -z "$SUBJECT" ]; then
        log_error "Subject required for --get-versions"
        exit 1
    fi
    get_all_versions "$SUBJECT"
    exit 0
fi

# Get compatibility mode
if [ "$GET_MODE" = true ]; then
    if [ -z "$SUBJECT" ]; then
        log_error "Subject required for --get-mode"
        exit 1
    fi
    get_compatibility_mode "$SUBJECT"
    exit 0
fi

# Check compatibility mode
if [ -z "$SUBJECT" ] || [ -z "$NEW_SCHEMA_FILE" ]; then
    log_error "Subject and schema file are required"
    echo ""
    usage
    exit 1
fi

log_info "Subject: $SUBJECT"
log_info "Schema file: $NEW_SCHEMA_FILE"
echo ""

if check_compatibility "$SUBJECT" "$NEW_SCHEMA_FILE"; then
    echo ""
    log_success "==================================================================="
    log_success "Compatibility Check PASSED"
    log_success "==================================================================="
    echo ""
    log_info "Next steps:"
    echo "  1. Apply schema change to ScyllaDB"
    echo "  2. Schema will be automatically registered by connector"
    echo "  3. Verify replication continues normally"
    exit 0
else
    echo ""
    log_error "==================================================================="
    log_error "Compatibility Check FAILED"
    log_error "==================================================================="
    echo ""
    log_warning "Do NOT apply this schema change!"
    log_info "Options:"
    echo "  1. Modify schema to be backward compatible (add defaults)"
    echo "  2. Change compatibility mode (not recommended)"
    echo "  3. Create new topic for incompatible schema"
    exit 1
fi
