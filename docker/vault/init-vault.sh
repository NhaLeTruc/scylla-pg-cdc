#!/usr/bin/env bash

# ============================================================================
# Vault Initialization Script for CDC Pipeline
# ============================================================================
# Initializes HashiCorp Vault with policies and secrets for the CDC pipeline
#
# Usage:
#   ./docker/vault/init-vault.sh
#
# Environment Variables:
#   VAULT_ADDR        Vault address (default: http://localhost:8200)
#   VAULT_TOKEN       Vault root token (required)
# ============================================================================

set -euo pipefail

# Configuration
VAULT_ADDR="${VAULT_ADDR:-http://localhost:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-dev-token}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

wait_for_vault() {
    log_info "Waiting for Vault to be ready..."
    local retries=30
    local count=0

    while [ $count -lt $retries ]; do
        if vault status > /dev/null 2>&1; then
            log_success "Vault is ready"
            return 0
        fi
        count=$((count + 1))
        echo -n "."
        sleep 2
    done

    log_error "Vault is not available at ${VAULT_ADDR}"
    exit 1
}

enable_kv_secrets_engine() {
    log_info "Enabling KV secrets engine..."

    if vault secrets list | grep -q "^secret/"; then
        log_warn "KV secrets engine already enabled"
    else
        vault secrets enable -version=2 -path=secret kv
        log_success "KV secrets engine enabled"
    fi
}

create_policies() {
    log_info "Creating Vault policies..."

    # Admin policy
    if [ -f "/vault/policies/admin-policy.hcl" ]; then
        vault policy write admin /vault/policies/admin-policy.hcl
        log_success "Created admin policy"
    fi

    # CDC policy
    if [ -f "/vault/policies/cdc-policy.hcl" ]; then
        vault policy write cdc /vault/policies/cdc-policy.hcl
        log_success "Created cdc policy"
    fi

    # Read-only policy
    if [ -f "/vault/policies/readonly-policy.hcl" ]; then
        vault policy write readonly /vault/policies/readonly-policy.hcl
        log_success "Created readonly policy"
    fi
}

create_secrets() {
    log_info "Creating secrets..."

    # ScyllaDB credentials
    log_info "Creating ScyllaDB credentials..."
    vault kv put secret/scylla-credentials \
        username="${SCYLLA_USER:-cassandra}" \
        password="${SCYLLA_PASSWORD:-cassandra}" \
        contact_points="${SCYLLA_CONTACT_POINTS:-scylla:9042}"
    log_success "ScyllaDB credentials created"

    # PostgreSQL credentials
    log_info "Creating PostgreSQL credentials..."
    vault kv put secret/postgres-credentials \
        username="${POSTGRES_USER:-postgres}" \
        password="${POSTGRES_PASSWORD:-postgres}" \
        database="${POSTGRES_DB:-warehouse}" \
        host="${POSTGRES_HOST:-postgres}" \
        port="${POSTGRES_PORT:-5432}"
    log_success "PostgreSQL credentials created"

    # Kafka credentials (for future use)
    log_info "Creating Kafka credentials..."
    vault kv put secret/kafka-credentials \
        bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" \
        sasl_mechanism="${KAFKA_SASL_MECHANISM:-PLAIN}" \
        username="${KAFKA_USERNAME:-}" \
        password="${KAFKA_PASSWORD:-}"
    log_success "Kafka credentials created"

    # Schema Registry credentials
    log_info "Creating Schema Registry credentials..."
    vault kv put secret/schema-registry-credentials \
        url="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}" \
        username="${SCHEMA_REGISTRY_USERNAME:-}" \
        password="${SCHEMA_REGISTRY_PASSWORD:-}"
    log_success "Schema Registry credentials created"

    # Grafana credentials
    log_info "Creating Grafana credentials..."
    vault kv put secret/grafana-credentials \
        admin_user="${GRAFANA_ADMIN_USER:-admin}" \
        admin_password="${GRAFANA_ADMIN_PASSWORD:-admin}"
    log_success "Grafana credentials created"

    # Prometheus credentials (if needed)
    log_info "Creating Prometheus credentials..."
    vault kv put secret/prometheus-credentials \
        url="${PROMETHEUS_URL:-http://prometheus:9090}"
    log_success "Prometheus credentials created"
}

create_tokens() {
    log_info "Creating application tokens..."

    # Create token for CDC pipeline with cdc policy
    local cdc_token
    cdc_token=$(vault token create -policy=cdc -format=json | jq -r '.auth.client_token')

    log_success "CDC pipeline token created"
    log_info "CDC Token: ${cdc_token}"

    # Store token for easy retrieval
    vault kv put secret/tokens/cdc-pipeline token="${cdc_token}"

    # Create readonly token for monitoring
    local readonly_token
    readonly_token=$(vault token create -policy=readonly -format=json | jq -r '.auth.client_token')

    log_success "Read-only token created"
    log_info "Readonly Token: ${readonly_token}"

    vault kv put secret/tokens/readonly token="${readonly_token}"
}

verify_setup() {
    log_info "Verifying Vault setup..."

    # Check policies
    log_info "Checking policies..."
    vault policy list

    # Check secrets
    log_info "Checking secrets..."
    vault kv list secret/

    # Verify secret access
    log_info "Verifying secret: scylla-credentials"
    vault kv get secret/scylla-credentials > /dev/null

    log_info "Verifying secret: postgres-credentials"
    vault kv get secret/postgres-credentials > /dev/null

    log_success "Vault verification completed"
}

show_summary() {
    log_info "===================================================="
    log_info "Vault Initialization Complete"
    log_info "===================================================="
    log_info "Vault Address: ${VAULT_ADDR}"
    log_info ""
    log_info "Policies created:"
    log_info "  - admin: Full administrative access"
    log_info "  - cdc: Access to CDC pipeline secrets"
    log_info "  - readonly: Read-only access"
    log_info ""
    log_info "Secrets created:"
    log_info "  - secret/scylla-credentials"
    log_info "  - secret/postgres-credentials"
    log_info "  - secret/kafka-credentials"
    log_info "  - secret/schema-registry-credentials"
    log_info "  - secret/grafana-credentials"
    log_info "  - secret/prometheus-credentials"
    log_info ""
    log_info "Application tokens stored in:"
    log_info "  - secret/tokens/cdc-pipeline"
    log_info "  - secret/tokens/readonly"
    log_info "===================================================="
}

# ============================================================================
# Main Execution
# ============================================================================

export VAULT_ADDR
export VAULT_TOKEN

log_info "Starting Vault initialization..."
log_info "Vault Address: ${VAULT_ADDR}"

# Wait for Vault to be available
wait_for_vault

# Enable secrets engine
enable_kv_secrets_engine

# Create policies
create_policies

# Create secrets
create_secrets

# Create tokens
create_tokens

# Verify setup
verify_setup

# Show summary
show_summary

log_success "Vault initialization completed successfully!"
