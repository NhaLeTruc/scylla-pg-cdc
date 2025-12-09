#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Local Development Setup
# ==============================================================================
# Description: Initialize Docker environment and verify prerequisites
# Usage: ./scripts/setup-local.sh
# ==============================================================================

set -euo pipefail

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ==============================================================================
# Helper Functions
# ==============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    local cmd=$1
    local name=${2:-$cmd}
    if ! command -v "$cmd" &> /dev/null; then
        log_error "$name is not installed or not in PATH"
        return 1
    else
        local version
        version=$($cmd --version 2>&1 | head -n 1)
        log_success "$name is available: $version"
        return 0
    fi
}

check_docker_compose_version() {
    local version
    if command -v docker-compose &> /dev/null; then
        version=$(docker-compose --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)
        log_success "docker-compose is available: v$version"
        return 0
    elif docker compose version &> /dev/null 2>&1; then
        version=$(docker compose version --short)
        log_success "docker compose (plugin) is available: v$version"
        return 0
    else
        log_error "docker-compose or docker compose plugin is not available"
        return 1
    fi
}

# ==============================================================================
# Prerequisite Checks
# ==============================================================================

log_info "==================================================================="
log_info "ScyllaDB to Postgres CDC Pipeline - Local Setup"
log_info "==================================================================="
echo ""

log_info "Step 1/6: Checking prerequisites..."
echo ""

# Track if all prerequisites are met
ALL_PREREQS_MET=true

# Check Docker
if ! check_command "docker" "Docker"; then
    ALL_PREREQS_MET=false
    log_error "Please install Docker: https://docs.docker.com/get-docker/"
fi

# Check Docker Compose
if ! check_docker_compose_version; then
    ALL_PREREQS_MET=false
    log_error "Please install Docker Compose: https://docs.docker.com/compose/install/"
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    log_error "Docker daemon is not running. Please start Docker."
    ALL_PREREQS_MET=false
else
    log_success "Docker daemon is running"
fi

# Check for optional but recommended tools
log_info "Checking optional tools..."
check_command "curl" "curl" || log_warning "curl not found (recommended for health checks)"
check_command "jq" "jq" || log_warning "jq not found (recommended for JSON processing)"
check_command "psql" "PostgreSQL client" || log_warning "psql not found (recommended for database access)"
check_command "cqlsh" "Cassandra/Scylla client" || log_warning "cqlsh not found (recommended for ScyllaDB access)"

if [ "$ALL_PREREQS_MET" = false ]; then
    log_error "Some required prerequisites are missing. Please install them and try again."
    exit 1
fi

echo ""
log_success "All required prerequisites are met!"
echo ""

# ==============================================================================
# Environment Setup
# ==============================================================================

log_info "Step 2/6: Setting up environment configuration..."
echo ""

cd "$PROJECT_ROOT"

# Check if .env file exists
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        log_info "Creating .env file from .env.example..."
        cp .env.example .env
        log_success ".env file created"
        log_warning "Please review and customize .env file if needed"
    else
        log_error ".env.example file not found"
        exit 1
    fi
else
    log_info ".env file already exists"
fi

# Validate critical environment variables
if [ -f .env ]; then
    log_info "Validating environment configuration..."
    source .env

    # Check for critical variables
    CRITICAL_VARS=(
        "POSTGRES_DB"
        "POSTGRES_USER"
        "POSTGRES_PASSWORD"
        "KAFKA_BROKERS"
        "SCHEMA_REGISTRY_URL"
        "VAULT_ADDR"
        "VAULT_TOKEN"
    )

    for var in "${CRITICAL_VARS[@]}"; do
        if [ -z "${!var:-}" ]; then
            log_warning "Environment variable $var is not set in .env"
        fi
    done

    log_success "Environment configuration validated"
fi

echo ""

# ==============================================================================
# Docker Environment Check
# ==============================================================================

log_info "Step 3/6: Checking Docker environment..."
echo ""

# Check available disk space
AVAILABLE_SPACE=$(df -BG "$PROJECT_ROOT" | awk 'NR==2 {print $4}' | sed 's/G//')
REQUIRED_SPACE=10

if [ "$AVAILABLE_SPACE" -lt "$REQUIRED_SPACE" ]; then
    log_warning "Available disk space: ${AVAILABLE_SPACE}GB (recommended: ${REQUIRED_SPACE}GB+)"
else
    log_success "Available disk space: ${AVAILABLE_SPACE}GB"
fi

# Check Docker resources
log_info "Checking Docker resource limits..."
DOCKER_MEM=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo "0")
DOCKER_MEM_GB=$((DOCKER_MEM / 1024 / 1024 / 1024))

if [ "$DOCKER_MEM_GB" -lt 8 ]; then
    log_warning "Docker memory: ${DOCKER_MEM_GB}GB (recommended: 8GB+)"
    log_warning "Consider increasing Docker memory allocation in Docker settings"
else
    log_success "Docker memory: ${DOCKER_MEM_GB}GB"
fi

echo ""

# ==============================================================================
# Clean Up Existing Containers (Optional)
# ==============================================================================

log_info "Step 4/6: Checking for existing containers..."
echo ""

# Check if containers are already running
RUNNING_CONTAINERS=$(docker compose -f docker/docker-compose.yml ps -q 2>/dev/null | wc -l)

if [ "$RUNNING_CONTAINERS" -gt 0 ]; then
    log_warning "Found $RUNNING_CONTAINERS running container(s) from this project"
    read -p "Do you want to stop and remove existing containers? (y/N): " -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Stopping and removing existing containers..."
        docker compose -f docker/docker-compose.yml down -v
        log_success "Existing containers removed"
    else
        log_info "Keeping existing containers"
    fi
else
    log_info "No existing containers found"
fi

echo ""

# ==============================================================================
# Initialize Docker Volumes and Networks
# ==============================================================================

log_info "Step 5/6: Initializing Docker infrastructure..."
echo ""

# Create network if it doesn't exist
if ! docker network inspect cdc-network &>/dev/null; then
    log_info "Creating Docker network: cdc-network..."
    docker network create cdc-network
    log_success "Network created"
else
    log_info "Docker network cdc-network already exists"
fi

# Pull Docker images to avoid timeout during first start
log_info "Pulling Docker images (this may take a few minutes)..."
docker compose -f docker/docker-compose.yml pull --quiet

log_success "Docker images pulled successfully"
echo ""

# ==============================================================================
# Build Custom Images
# ==============================================================================

log_info "Building custom Docker images..."
docker compose -f docker/docker-compose.yml build --quiet

log_success "Custom images built successfully"
echo ""

# ==============================================================================
# Start Services
# ==============================================================================

log_info "Step 6/6: Starting services..."
echo ""

log_info "Starting core services (this may take 2-3 minutes)..."
log_info "Services will start in the following order:"
log_info "  1. Zookeeper"
log_info "  2. Kafka"
log_info "  3. Schema Registry"
log_info "  4. Kafka Connect"
log_info "  5. ScyllaDB"
log_info "  6. PostgreSQL"
log_info "  7. Vault"
log_info "  8. Monitoring stack (Prometheus, Grafana, Jaeger)"
echo ""

# Start services in detached mode
docker compose -f docker/docker-compose.yml up -d

log_success "Services started successfully"
echo ""

# ==============================================================================
# Wait for Services to be Healthy
# ==============================================================================

log_info "Waiting for services to become healthy..."
log_info "(This may take 1-2 minutes for all services to initialize)"
echo ""

MAX_WAIT=180  # 3 minutes
ELAPSED=0
CHECK_INTERVAL=5

while [ $ELAPSED -lt $MAX_WAIT ]; do
    UNHEALTHY=$(docker compose -f docker/docker-compose.yml ps --format json 2>/dev/null | \
                jq -r 'select(.Health != "healthy" and .Health != "") | .Service' 2>/dev/null | wc -l)

    if [ "$UNHEALTHY" -eq 0 ]; then
        log_success "All services are healthy!"
        break
    fi

    log_info "Waiting for $UNHEALTHY service(s) to become healthy... (${ELAPSED}s elapsed)"
    sleep $CHECK_INTERVAL
    ELAPSED=$((ELAPSED + CHECK_INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    log_warning "Some services may not be fully healthy yet"
    log_info "You can check service status with: docker compose -f docker/docker-compose.yml ps"
fi

echo ""

# ==============================================================================
# Initialize Vault
# ==============================================================================

log_info "Initializing HashiCorp Vault..."
echo ""

# Wait a moment for Vault to be ready
sleep 2

# Execute Vault initialization script
if docker exec vault sh -c '[ -f /vault/init-vault.sh ]' 2>/dev/null; then
    log_info "Running Vault initialization script..."
    docker exec vault sh /vault/init-vault.sh
    log_success "Vault initialized successfully"
else
    log_warning "Vault initialization script not found"
fi

echo ""

# ==============================================================================
# Summary and Next Steps
# ==============================================================================

log_success "========================================================================="
log_success "Local Development Environment Setup Complete!"
log_success "========================================================================="
echo ""

log_info "Service Endpoints:"
echo "  - ScyllaDB CQL:           localhost:9042"
echo "  - ScyllaDB REST API:      http://localhost:10000"
echo "  - PostgreSQL:             localhost:5432"
echo "  - Kafka:                  localhost:9092"
echo "  - Schema Registry:        http://localhost:8081"
echo "  - Kafka Connect:          http://localhost:8083"
echo "  - Vault:                  http://localhost:8200"
echo "  - Prometheus:             http://localhost:9090"
echo "  - Grafana:                http://localhost:3000 (admin/admin)"
echo "  - Jaeger UI:              http://localhost:16686"
echo ""

log_info "Next Steps:"
echo "  1. Verify service health:     ./scripts/health-check.sh"
echo "  2. Deploy Kafka connectors:   ./scripts/deploy-connectors.sh"
echo "  3. Monitor connector status:  ./scripts/monitor-connectors.sh"
echo "  4. View logs:                 docker compose -f docker/docker-compose.yml logs -f [service]"
echo "  5. Stop services:             ./scripts/teardown-local.sh"
echo ""

log_info "Quick Access Commands:"
echo "  - ScyllaDB:   docker exec -it scylla cqlsh"
echo "  - PostgreSQL: docker exec -it postgres psql -U postgres -d warehouse"
echo "  - Kafka:      docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic [topic]"
echo ""

log_info "Documentation:"
echo "  - Architecture:     docs/architecture.md"
echo "  - Troubleshooting:  docs/troubleshooting.md"
echo "  - Runbook:          docs/runbook.md"
echo ""

log_success "Happy developing! 🚀"
