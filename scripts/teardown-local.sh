#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Local Environment Teardown
# ==============================================================================
# Description: Clean up Docker environment and optionally remove volumes/networks
# Usage: ./scripts/teardown-local.sh [--keep-volumes] [--keep-data] [--force]
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
KEEP_VOLUMES=false
KEEP_DATA=false
FORCE_MODE=false
REMOVE_IMAGES=false

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

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Stop and clean up the CDC pipeline local development environment.

OPTIONS:
    -h, --help              Show this help message
    -v, --keep-volumes      Keep Docker volumes (preserves data)
    -d, --keep-data         Alias for --keep-volumes
    -f, --force             Skip confirmation prompts
    -i, --remove-images     Also remove Docker images (full cleanup)

EXAMPLES:
    $0                      # Stop containers and remove volumes (with confirmation)
    $0 --keep-volumes       # Stop containers but keep data
    $0 --force              # Stop and remove everything without asking
    $0 -f -i                # Force remove including Docker images

DESCRIPTION:
    This script stops all running containers and optionally removes:
    - Docker containers
    - Docker volumes (unless --keep-volumes specified)
    - Docker network
    - Docker images (if --remove-images specified)

    By default, you will be prompted to confirm before removing volumes.
EOF
}

confirm_action() {
    local prompt=$1
    if [ "$FORCE_MODE" = true ]; then
        return 0
    fi
    read -p "$prompt (y/N): " -r
    echo
    [[ $REPLY =~ ^[Yy]$ ]]
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
        -v|--keep-volumes)
            KEEP_VOLUMES=true
            shift
            ;;
        -d|--keep-data)
            KEEP_VOLUMES=true
            shift
            ;;
        -f|--force)
            FORCE_MODE=true
            shift
            ;;
        -i|--remove-images)
            REMOVE_IMAGES=true
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
# Main Teardown Logic
# ==============================================================================

cd "$PROJECT_ROOT"

log_info "==================================================================="
log_info "ScyllaDB to Postgres CDC Pipeline - Environment Teardown"
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
    log_error "Are you in the correct directory?"
    exit 1
fi

# Check if any containers are running
RUNNING_CONTAINERS=$(docker compose -f docker/docker-compose.yml ps -q 2>/dev/null | wc -l)

if [ "$RUNNING_CONTAINERS" -eq 0 ]; then
    log_info "No running containers found from this project"
else
    log_info "Found $RUNNING_CONTAINERS running container(s)"
fi

# Display what will be removed
echo ""
log_info "Teardown plan:"
echo "  - Stop all containers: YES"
echo "  - Remove containers: YES"
if [ "$KEEP_VOLUMES" = true ]; then
    echo "  - Remove volumes: NO (--keep-volumes specified)"
else
    echo "  - Remove volumes: YES (all data will be lost)"
fi
echo "  - Remove network: YES"
if [ "$REMOVE_IMAGES" = true ]; then
    echo "  - Remove images: YES (--remove-images specified)"
else
    echo "  - Remove images: NO"
fi
echo ""

# Confirm before proceeding
if [ "$KEEP_VOLUMES" = false ]; then
    log_warning "This will delete all data in Docker volumes!"
    if ! confirm_action "Are you sure you want to proceed?"; then
        log_info "Teardown cancelled"
        exit 0
    fi
fi

# ==============================================================================
# Stop and Remove Containers
# ==============================================================================

log_info "Stopping containers..."
echo ""

if [ "$RUNNING_CONTAINERS" -gt 0 ]; then
    docker compose -f docker/docker-compose.yml stop
    log_success "Containers stopped"
else
    log_info "No containers to stop"
fi

echo ""
log_info "Removing containers..."

if [ "$KEEP_VOLUMES" = true ]; then
    docker compose -f docker/docker-compose.yml down
else
    docker compose -f docker/docker-compose.yml down -v --remove-orphans
fi

log_success "Containers removed"

# ==============================================================================
# Remove Network
# ==============================================================================

echo ""
log_info "Checking network..."

if docker network inspect cdc-network &>/dev/null; then
    log_info "Removing Docker network: cdc-network"
    docker network rm cdc-network 2>/dev/null || log_warning "Network may be in use or already removed"
    log_success "Network removed"
else
    log_info "Network cdc-network not found (already removed)"
fi

# ==============================================================================
# Optional: Remove Images
# ==============================================================================

if [ "$REMOVE_IMAGES" = true ]; then
    echo ""
    log_info "Removing Docker images..."

    # List of images used by this project
    IMAGES=(
        "confluentinc/cp-zookeeper:7.5.3"
        "confluentinc/cp-kafka:7.5.3"
        "confluentinc/cp-schema-registry:7.5.3"
        "scylladb/scylla:5.4"
        "postgres:15-alpine"
        "hashicorp/vault:1.15"
        "prom/prometheus:v2.48.1"
        "grafana/grafana:10.2.3"
        "jaegertracing/all-in-one:1.53"
    )

    # Custom built images
    CUSTOM_IMAGES=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep -E "kafka-connect|cdc" || true)

    for image in "${IMAGES[@]}"; do
        if docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "^${image}$"; then
            log_info "Removing image: $image"
            docker rmi "$image" 2>/dev/null || log_warning "Failed to remove $image (may be in use)"
        fi
    done

    # Remove custom images
    if [ -n "$CUSTOM_IMAGES" ]; then
        for image in $CUSTOM_IMAGES; do
            log_info "Removing custom image: $image"
            docker rmi "$image" 2>/dev/null || log_warning "Failed to remove $image"
        done
    fi

    log_success "Images removed"
fi

# ==============================================================================
# Volume Information
# ==============================================================================

if [ "$KEEP_VOLUMES" = true ]; then
    echo ""
    log_info "Volume Status (preserved):"
    echo ""

    # List volumes related to this project
    VOLUMES=$(docker volume ls --format "{{.Name}}" | grep -E "scylla-pg-cdc|zookeeper|kafka|postgres|scylla|vault|prometheus|grafana|jaeger" || true)

    if [ -n "$VOLUMES" ]; then
        for volume in $VOLUMES; do
            SIZE=$(docker volume inspect "$volume" --format "{{.Name}}" 2>/dev/null || echo "unknown")
            if [ "$SIZE" != "unknown" ]; then
                log_info "  - $volume (preserved)"
            fi
        done
        echo ""
        log_warning "Data volumes are preserved. To remove them manually:"
        echo "  docker volume rm $VOLUMES"
    else
        log_info "No volumes found"
    fi
fi

# ==============================================================================
# Cleanup Verification
# ==============================================================================

echo ""
log_info "Verifying cleanup..."

# Check for remaining containers
REMAINING_CONTAINERS=$(docker compose -f docker/docker-compose.yml ps -q 2>/dev/null | wc -l)
if [ "$REMAINING_CONTAINERS" -eq 0 ]; then
    log_success "All containers removed"
else
    log_warning "$REMAINING_CONTAINERS container(s) still present"
fi

# Check for network
if docker network inspect cdc-network &>/dev/null; then
    log_warning "Network cdc-network still exists"
else
    log_success "Network removed"
fi

# ==============================================================================
# Disk Space Information
# ==============================================================================

echo ""
log_info "Disk space summary:"

TOTAL_SPACE=$(df -h "$PROJECT_ROOT" | awk 'NR==2 {print $2}')
AVAILABLE_SPACE=$(df -h "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
USE_PERCENT=$(df -h "$PROJECT_ROOT" | awk 'NR==2 {print $5}')

echo "  Total: $TOTAL_SPACE"
echo "  Available: $AVAILABLE_SPACE"
echo "  Used: $USE_PERCENT"

# Docker disk usage
echo ""
log_info "Docker disk usage:"
docker system df

# ==============================================================================
# Summary
# ==============================================================================

echo ""
log_success "========================================================================="
log_success "Environment Teardown Complete!"
log_success "========================================================================="
echo ""

if [ "$KEEP_VOLUMES" = true ]; then
    log_info "Data volumes were preserved. Your data is safe."
    log_info "To start the environment again: ./scripts/setup-local.sh"
else
    log_info "All containers and data have been removed."
    log_info "To start fresh: ./scripts/setup-local.sh"
fi

echo ""
log_info "Optional cleanup commands:"
echo "  - Remove unused Docker resources:  docker system prune -a"
echo "  - Remove all volumes:              docker volume prune -f"
echo "  - Remove everything:               docker system prune -a --volumes"
echo ""

if [ "$REMOVE_IMAGES" = false ]; then
    log_info "Tip: Use --remove-images flag to also remove Docker images"
fi

log_success "Done! 🎉"
