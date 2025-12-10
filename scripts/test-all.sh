#!/usr/bin/env bash

#
# Run All Tests for CDC Pipeline
#
# Executes unit tests, integration tests, and contract tests with coverage reporting
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
TEST_TYPE="${TEST_TYPE:-all}"
COVERAGE="${COVERAGE:-true}"
VERBOSE="${VERBOSE:-false}"
FAIL_FAST="${FAIL_FAST:-false}"
PARALLEL="${PARALLEL:-false}"

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Run all tests for the CDC pipeline.

OPTIONS:
    --type TYPE         Test type: all|unit|integration|contract (default: all)
    --no-coverage       Disable coverage reporting
    --verbose           Verbose output
    --fail-fast         Stop on first failure
    --parallel          Run tests in parallel
    -h, --help          Show this help

EXAMPLES:
    # Run all tests
    $0

    # Run only unit tests
    $0 --type unit

    # Run integration tests with verbose output
    $0 --type integration --verbose

    # Run tests in parallel
    $0 --parallel

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --type)
            TEST_TYPE="$2"
            shift 2
            ;;
        --no-coverage)
            COVERAGE=false
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --fail-fast)
            FAIL_FAST=true
            shift
            ;;
        --parallel)
            PARALLEL=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    log_error "pytest not found. Install with: pip install -r requirements.txt"
    exit 1
fi

# Build pytest command
PYTEST_CMD="pytest"

if [[ "$VERBOSE" == "true" ]]; then
    PYTEST_CMD="$PYTEST_CMD -vv"
fi

if [[ "$FAIL_FAST" == "true" ]]; then
    PYTEST_CMD="$PYTEST_CMD -x"
fi

if [[ "$PARALLEL" == "true" ]]; then
    PYTEST_CMD="$PYTEST_CMD -n auto"
fi

if [[ "$COVERAGE" == "false" ]]; then
    PYTEST_CMD="$PYTEST_CMD --no-cov"
fi

# Function to run tests
run_tests() {
    local test_type=$1
    local marker=""

    case $test_type in
        unit)
            marker="-m unit"
            log_info "Running unit tests..."
            ;;
        integration)
            marker="-m integration"
            log_info "Running integration tests..."
            log_warn "Integration tests require Docker services to be running"
            ;;
        contract)
            marker="-m contract"
            log_info "Running contract tests..."
            ;;
        all)
            marker=""
            log_info "Running all tests..."
            ;;
        *)
            log_error "Unknown test type: $test_type"
            exit 1
            ;;
    esac

    # Execute tests
    echo
    if $PYTEST_CMD $marker; then
        log_success "$test_type tests passed"
        return 0
    else
        log_error "$test_type tests failed"
        return 1
    fi
}

# Check Docker services for integration tests
check_docker_services() {
    if [[ "$TEST_TYPE" == "integration" ]] || [[ "$TEST_TYPE" == "all" ]]; then
        log_info "Checking Docker services..."

        if ! docker ps &> /dev/null; then
            log_error "Docker is not running"
            exit 1
        fi

        # Check if required services are running
        local services=("scylla" "postgres" "kafka" "kafka-connect")
        local all_running=true

        for service in "${services[@]}"; do
            if ! docker ps --format '{{.Names}}' | grep -q "$service"; then
                log_warn "Service $service is not running"
                all_running=false
            fi
        done

        if [[ "$all_running" == "false" ]]; then
            log_warn "Some services are not running. Start with: docker-compose up -d"
            log_warn "Integration tests may fail"
        else
            log_success "All required Docker services are running"
        fi
    fi
}

# Display test summary
display_summary() {
    local exit_code=$1

    echo
    echo "=========================================="
    echo "           TEST SUMMARY"
    echo "=========================================="

    if [[ $exit_code -eq 0 ]]; then
        log_success "All tests passed!"

        if [[ "$COVERAGE" == "true" ]]; then
            echo
            log_info "Coverage report generated:"
            log_info "  - Terminal: See above"
            log_info "  - HTML: coverage_html/index.html"
            echo
            log_info "View HTML coverage: open coverage_html/index.html"
        fi
    else
        log_error "Some tests failed (exit code: $exit_code)"
        log_info "Re-run with --verbose for more details"
    fi

    echo "=========================================="
}

# Main execution
main() {
    log_info "ScyllaDB to PostgreSQL CDC Pipeline - Test Suite"
    echo

    # Check Docker services if needed
    check_docker_services

    # Run tests
    local exit_code=0

    case $TEST_TYPE in
        all)
            # Run tests in sequence: unit -> contract -> integration
            log_info "Running tests in sequence: unit → contract → integration"
            echo

            run_tests "unit" || exit_code=$?

            if [[ $exit_code -eq 0 ]] || [[ "$FAIL_FAST" == "false" ]]; then
                echo
                run_tests "contract" || exit_code=$?
            fi

            if [[ $exit_code -eq 0 ]] || [[ "$FAIL_FAST" == "false" ]]; then
                echo
                run_tests "integration" || exit_code=$?
            fi
            ;;
        *)
            run_tests "$TEST_TYPE" || exit_code=$?
            ;;
    esac

    # Display summary
    display_summary $exit_code

    exit $exit_code
}

# Run main function
main
