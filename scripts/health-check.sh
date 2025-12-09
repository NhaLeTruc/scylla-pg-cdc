#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Health Check Script
# ==============================================================================
# Description: Verify all service health endpoints and connectivity
# Usage: ./scripts/health-check.sh [--verbose] [--wait] [--service SERVICE_NAME]
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
VERBOSE=false
WAIT_MODE=false
SPECIFIC_SERVICE=""
MAX_WAIT=300  # 5 minutes
CHECK_INTERVAL=5

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

Verify health of all CDC pipeline services.

OPTIONS:
    -h, --help              Show this help message
    -v, --verbose           Enable verbose output
    -w, --wait              Wait for services to become healthy
    -s, --service SERVICE   Check only specific service

EXAMPLES:
    $0                      # Check all services once
    $0 --verbose            # Check with detailed output
    $0 --wait               # Wait until all services are healthy
    $0 -s kafka             # Check only Kafka service

EXIT CODES:
    0 - All services healthy
    1 - One or more services unhealthy
    2 - Invalid arguments
EOF
}

# ==============================================================================
# Service Health Check Functions
# ==============================================================================

check_docker_service() {
    local service=$1
    local container=$2

    log_verbose "Checking Docker container status for $service..."

    if ! docker ps --filter "name=$container" --filter "status=running" --format "{{.Names}}" | grep -q "^${container}$"; then
        log_error "$service: Container not running"
        return 1
    fi

    # Check Docker health status if available
    local health_status
    health_status=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "none")

    if [ "$health_status" = "healthy" ]; then
        log_verbose "$service: Docker healthcheck passed"
        return 0
    elif [ "$health_status" = "none" ]; then
        log_verbose "$service: No Docker healthcheck defined"
        return 0
    else
        log_verbose "$service: Docker healthcheck status: $health_status"
        return 1
    fi
}

check_zookeeper() {
    log_verbose "Checking Zookeeper..."

    if ! check_docker_service "Zookeeper" "zookeeper"; then
        return 1
    fi

    # Check Zookeeper port
    if docker exec zookeeper bash -c "echo ruok | nc localhost 2181" 2>/dev/null | grep -q "imok"; then
        log_success "Zookeeper: Healthy (port 2181)"
        return 0
    else
        log_error "Zookeeper: Unhealthy - port check failed"
        return 1
    fi
}

check_kafka() {
    log_verbose "Checking Kafka..."

    if ! check_docker_service "Kafka" "kafka"; then
        return 1
    fi

    # Check Kafka broker API
    if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        log_success "Kafka: Healthy (broker responding)"

        if [ "$VERBOSE" = true ]; then
            local topic_count
            topic_count=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | wc -l)
            log_verbose "Kafka: $topic_count topic(s) available"
        fi
        return 0
    else
        log_error "Kafka: Unhealthy - broker not responding"
        return 1
    fi
}

check_schema_registry() {
    log_verbose "Checking Schema Registry..."

    if ! check_docker_service "Schema Registry" "schema-registry"; then
        return 1
    fi

    # Check Schema Registry HTTP endpoint
    if curl -sf http://localhost:8081/ &>/dev/null; then
        log_success "Schema Registry: Healthy (http://localhost:8081)"

        if [ "$VERBOSE" = true ]; then
            local schema_count
            schema_count=$(curl -sf http://localhost:8081/subjects 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
            log_verbose "Schema Registry: $schema_count schema(s) registered"
        fi
        return 0
    else
        log_error "Schema Registry: Unhealthy - HTTP endpoint not responding"
        return 1
    fi
}

check_kafka_connect() {
    log_verbose "Checking Kafka Connect..."

    if ! check_docker_service "Kafka Connect" "kafka-connect"; then
        return 1
    fi

    # Check Kafka Connect REST API
    if curl -sf http://localhost:8083/ &>/dev/null; then
        log_success "Kafka Connect: Healthy (http://localhost:8083)"

        if [ "$VERBOSE" = true ]; then
            local connector_response
            connector_response=$(curl -sf http://localhost:8083/connectors 2>/dev/null)
            local connector_count
            connector_count=$(echo "$connector_response" | jq 'length' 2>/dev/null || echo "0")
            log_verbose "Kafka Connect: $connector_count connector(s) deployed"

            # Check each connector status
            if [ "$connector_count" -gt 0 ]; then
                local connectors
                connectors=$(echo "$connector_response" | jq -r '.[]' 2>/dev/null)
                for connector in $connectors; do
                    local status
                    status=$(curl -sf "http://localhost:8083/connectors/$connector/status" 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN")
                    if [ "$status" = "RUNNING" ]; then
                        log_verbose "  - $connector: RUNNING"
                    else
                        log_warning "  - $connector: $status"
                    fi
                done
            fi
        fi
        return 0
    else
        log_error "Kafka Connect: Unhealthy - REST API not responding"
        return 1
    fi
}

check_scylla() {
    log_verbose "Checking ScyllaDB..."

    if ! check_docker_service "ScyllaDB" "scylla"; then
        return 1
    fi

    # Check ScyllaDB CQL interface
    if docker exec scylla cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        log_success "ScyllaDB: Healthy (CQL port 9042)"

        if [ "$VERBOSE" = true ]; then
            local keyspace_count
            keyspace_count=$(docker exec scylla cqlsh -e "DESCRIBE KEYSPACES" 2>/dev/null | wc -w)
            log_verbose "ScyllaDB: $keyspace_count keyspace(s) available"

            # Check REST API
            if curl -sf http://localhost:10000/storage_service/native_transport &>/dev/null; then
                log_verbose "ScyllaDB: REST API responding (http://localhost:10000)"
            fi
        fi
        return 0
    else
        log_error "ScyllaDB: Unhealthy - CQL interface not responding"
        return 1
    fi
}

check_postgres() {
    log_verbose "Checking PostgreSQL..."

    if ! check_docker_service "PostgreSQL" "postgres"; then
        return 1
    fi

    # Check PostgreSQL connection
    if docker exec postgres pg_isready -U postgres -d warehouse &>/dev/null; then
        log_success "PostgreSQL: Healthy (port 5432)"

        if [ "$VERBOSE" = true ]; then
            local table_count
            table_count=$(docker exec postgres psql -U postgres -d warehouse -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'" 2>/dev/null | tr -d ' ' || echo "0")
            log_verbose "PostgreSQL: $table_count table(s) in public schema"

            # Check cdc_metadata schema
            local cdc_schema_exists
            cdc_schema_exists=$(docker exec postgres psql -U postgres -d warehouse -t -c "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'cdc_metadata'" 2>/dev/null | tr -d ' ' || echo "0")
            if [ "$cdc_schema_exists" = "1" ]; then
                log_verbose "PostgreSQL: cdc_metadata schema exists"
            else
                log_warning "PostgreSQL: cdc_metadata schema not found"
            fi
        fi
        return 0
    else
        log_error "PostgreSQL: Unhealthy - database not responding"
        return 1
    fi
}

check_vault() {
    log_verbose "Checking HashiCorp Vault..."

    if ! check_docker_service "Vault" "vault"; then
        return 1
    fi

    # Check Vault status
    if docker exec vault vault status &>/dev/null; then
        log_success "Vault: Healthy (http://localhost:8200)"

        if [ "$VERBOSE" = true ]; then
            local vault_initialized
            vault_initialized=$(docker exec vault vault status -format=json 2>/dev/null | jq -r '.initialized' 2>/dev/null || echo "false")
            local vault_sealed
            vault_sealed=$(docker exec vault vault status -format=json 2>/dev/null | jq -r '.sealed' 2>/dev/null || echo "true")

            log_verbose "Vault: Initialized=$vault_initialized, Sealed=$vault_sealed"
        fi
        return 0
    else
        log_error "Vault: Unhealthy - status check failed"
        return 1
    fi
}

check_prometheus() {
    log_verbose "Checking Prometheus..."

    if ! check_docker_service "Prometheus" "prometheus"; then
        return 1
    fi

    # Check Prometheus HTTP endpoint
    if curl -sf http://localhost:9090/-/healthy &>/dev/null; then
        log_success "Prometheus: Healthy (http://localhost:9090)"

        if [ "$VERBOSE" = true ]; then
            local target_count
            target_count=$(curl -sf http://localhost:9090/api/v1/targets 2>/dev/null | jq '.data.activeTargets | length' 2>/dev/null || echo "0")
            log_verbose "Prometheus: $target_count active target(s)"
        fi
        return 0
    else
        log_error "Prometheus: Unhealthy - HTTP endpoint not responding"
        return 1
    fi
}

check_grafana() {
    log_verbose "Checking Grafana..."

    if ! check_docker_service "Grafana" "grafana"; then
        return 1
    fi

    # Check Grafana HTTP endpoint
    if curl -sf http://localhost:3000/api/health &>/dev/null; then
        log_success "Grafana: Healthy (http://localhost:3000)"

        if [ "$VERBOSE" = true ]; then
            local dashboard_count
            dashboard_count=$(curl -sf -u admin:admin http://localhost:3000/api/search?type=dash-db 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
            log_verbose "Grafana: $dashboard_count dashboard(s) available"
        fi
        return 0
    else
        log_error "Grafana: Unhealthy - HTTP endpoint not responding"
        return 1
    fi
}

check_jaeger() {
    log_verbose "Checking Jaeger..."

    if ! check_docker_service "Jaeger" "jaeger"; then
        return 1
    fi

    # Check Jaeger HTTP endpoint
    if curl -sf http://localhost:14269/ &>/dev/null; then
        log_success "Jaeger: Healthy (http://localhost:16686)"
        return 0
    else
        log_error "Jaeger: Unhealthy - HTTP endpoint not responding"
        return 1
    fi
}

# ==============================================================================
# Main Health Check Logic
# ==============================================================================

check_all_services() {
    local failed=0

    # Core services (required for pipeline)
    log_info "Checking core services..."
    echo ""

    check_zookeeper || ((failed++))
    check_kafka || ((failed++))
    check_schema_registry || ((failed++))
    check_kafka_connect || ((failed++))
    check_scylla || ((failed++))
    check_postgres || ((failed++))
    check_vault || ((failed++))

    echo ""

    # Monitoring services (optional)
    log_info "Checking monitoring services..."
    echo ""

    check_prometheus || log_warning "Prometheus is optional for basic operation"
    check_grafana || log_warning "Grafana is optional for basic operation"
    check_jaeger || log_warning "Jaeger is optional for basic operation"

    return $failed
}

check_single_service() {
    local service=$1

    case "$service" in
        zookeeper)
            check_zookeeper
            ;;
        kafka)
            check_kafka
            ;;
        schema-registry|schema_registry)
            check_schema_registry
            ;;
        kafka-connect|kafka_connect)
            check_kafka_connect
            ;;
        scylla|scylladb)
            check_scylla
            ;;
        postgres|postgresql)
            check_postgres
            ;;
        vault)
            check_vault
            ;;
        prometheus)
            check_prometheus
            ;;
        grafana)
            check_grafana
            ;;
        jaeger)
            check_jaeger
            ;;
        *)
            log_error "Unknown service: $service"
            log_info "Available services: zookeeper, kafka, schema-registry, kafka-connect, scylla, postgres, vault, prometheus, grafana, jaeger"
            return 2
            ;;
    esac
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
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -w|--wait)
            WAIT_MODE=true
            shift
            ;;
        -s|--service)
            SPECIFIC_SERVICE="$2"
            shift 2
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 2
            ;;
    esac
done

# ==============================================================================
# Main Execution
# ==============================================================================

cd "$PROJECT_ROOT"

log_info "==================================================================="
log_info "ScyllaDB to Postgres CDC Pipeline - Health Check"
log_info "==================================================================="
echo ""

# Check if Docker is running
if ! docker info &>/dev/null; then
    log_error "Docker daemon is not running"
    exit 1
fi

# Check if docker-compose.yml exists
if [ ! -f "docker/docker-compose.yml" ]; then
    log_error "docker/docker-compose.yml not found"
    exit 1
fi

# Wait mode
if [ "$WAIT_MODE" = true ]; then
    log_info "Waiting for services to become healthy (max ${MAX_WAIT}s)..."
    echo ""

    ELAPSED=0
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        if [ -n "$SPECIFIC_SERVICE" ]; then
            if check_single_service "$SPECIFIC_SERVICE"; then
                echo ""
                log_success "Service $SPECIFIC_SERVICE is healthy after ${ELAPSED}s"
                exit 0
            fi
        else
            if check_all_services; then
                echo ""
                log_success "All services are healthy after ${ELAPSED}s"
                exit 0
            fi
        fi

        log_info "Waiting... (${ELAPSED}s elapsed)"
        sleep $CHECK_INTERVAL
        ELAPSED=$((ELAPSED + CHECK_INTERVAL))
        echo ""
    done

    echo ""
    log_error "Timeout reached after ${MAX_WAIT}s - some services still unhealthy"
    exit 1
fi

# Normal check mode
if [ -n "$SPECIFIC_SERVICE" ]; then
    check_single_service "$SPECIFIC_SERVICE"
    exit_code=$?

    echo ""
    if [ $exit_code -eq 0 ]; then
        log_success "Service $SPECIFIC_SERVICE is healthy"
    else
        log_error "Service $SPECIFIC_SERVICE is unhealthy"
    fi
    exit $exit_code
else
    check_all_services
    failed=$?

    echo ""
    log_info "==================================================================="
    if [ $failed -eq 0 ]; then
        log_success "All core services are healthy! ✓"
    else
        log_error "$failed core service(s) are unhealthy"
        log_info "Run with --verbose flag for detailed diagnostics"
        log_info "Run with --wait flag to wait for services to become healthy"
    fi
    log_info "==================================================================="

    exit $failed
fi
