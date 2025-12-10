#!/usr/bin/env bash

#
# Chaos Testing Script for CDC Pipeline
#
# Uses Toxiproxy for failure injection testing
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
TOXIPROXY_HOST="${TOXIPROXY_HOST:-localhost:8474}"
DRY_RUN="${DRY_RUN:-false}"
DURATION="${DURATION:-60}"
REPORT_FILE="/tmp/chaos-test-report-$(date +%Y%m%d-%H%M%S).json"

# Safety check
ENVIRONMENT="${ENVIRONMENT:-development}"

if [[ "$ENVIRONMENT" == "production" ]]; then
    echo -e "${RED}[ERROR]${NC} Cannot run chaos tests in production!"
    echo "Set ENVIRONMENT=development or ENVIRONMENT=staging to proceed"
    exit 1
fi

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Chaos testing for CDC pipeline using Toxiproxy.

OPTIONS:
    --scenario NAME         Chaos scenario to run (required)
    --target SERVICE        Target service (scylla|postgres|kafka|kafka-connect)
    --duration SECONDS      Test duration (default: 60)
    --intensity LEVEL       Intensity: low|medium|high (default: medium)
    --dry-run               Show what would be done without executing
    -h, --help              Show this help

SCENARIOS:
    latency                 Add network latency
    bandwidth               Limit bandwidth
    connection-cut          Simulate network partition
    slow-close              Slow connection termination
    timeout                 Connection timeouts
    all                     Run all scenarios

EXAMPLES:
    # Test high latency on ScyllaDB
    $0 --scenario latency --target scylla --intensity high

    # Test network partition on Kafka
    $0 --scenario connection-cut --target kafka --duration 120

    # Run all chaos tests
    $0 --scenario all --target kafka-connect

EOF
    exit 0
}

# Check if Toxiproxy is running
check_toxiproxy() {
    log_info "Checking Toxiproxy availability..."

    if ! curl -sf "http://$TOXIPROXY_HOST/proxies" > /dev/null; then
        log_error "Toxiproxy not accessible at $TOXIPROXY_HOST"
        log_info "Start Toxiproxy: docker-compose up -d toxiproxy"
        exit 1
    fi

    log_success "Toxiproxy is running"
}

# Create proxy for service
create_proxy() {
    local service=$1
    local listen_port=$2
    local upstream_port=$3

    log_info "Creating Toxiproxy for $service..."

    curl -sf -X POST "http://$TOXIPROXY_HOST/proxies" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"$service\",
            \"listen\": \"0.0.0.0:$listen_port\",
            \"upstream\": \"$service:$upstream_port\",
            \"enabled\": true
        }" > /dev/null

    log_success "Proxy created for $service"
}

# Add latency toxic
add_latency() {
    local service=$1
    local intensity=$2

    local latency_ms=100
    local jitter_ms=50

    case $intensity in
        low)
            latency_ms=50
            jitter_ms=20
            ;;
        medium)
            latency_ms=200
            jitter_ms=100
            ;;
        high)
            latency_ms=1000
            jitter_ms=500
            ;;
    esac

    log_info "Adding latency toxic to $service (${latency_ms}ms ± ${jitter_ms}ms)..."

    curl -sf -X POST "http://$TOXIPROXY_HOST/proxies/$service/toxics" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"latency\",
            \"type\": \"latency\",
            \"attributes\": {
                \"latency\": $latency_ms,
                \"jitter\": $jitter_ms
            }
        }" > /dev/null

    log_success "Latency toxic added"
}

# Add bandwidth limit
add_bandwidth_limit() {
    local service=$1
    local intensity=$2

    local rate_kb=1000

    case $intensity in
        low)
            rate_kb=5000
            ;;
        medium)
            rate_kb=1000
            ;;
        high)
            rate_kb=100
            ;;
    esac

    log_info "Adding bandwidth limit to $service (${rate_kb} KB/s)..."

    curl -sf -X POST "http://$TOXIPROXY_HOST/proxies/$service/toxics" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"bandwidth\",
            \"type\": \"bandwidth\",
            \"attributes\": {
                \"rate\": $rate_kb
            }
        }" > /dev/null

    log_success "Bandwidth limit added"
}

# Simulate connection cut (network partition)
add_connection_cut() {
    local service=$1

    log_info "Simulating network partition for $service..."

    curl -sf -X POST "http://$TOXIPROXY_HOST/proxies/$service/toxics" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "connection_cut",
            "type": "timeout",
            "attributes": {
                "timeout": 0
            }
        }' > /dev/null

    log_success "Network partition simulated"
}

# Add slow close toxic
add_slow_close() {
    local service=$1

    log_info "Adding slow close toxic to $service..."

    curl -sf -X POST "http://$TOXIPROXY_HOST/proxies/$service/toxics" \
        -H "Content-Type: application/json" \
        -d '{
            "name": "slow_close",
            "type": "slow_close",
            "attributes": {
                "delay": 5000
            }
        }' > /dev/null

    log_success "Slow close toxic added"
}

# Remove all toxics
remove_toxics() {
    local service=$1

    log_info "Removing all toxics from $service..."

    curl -sf -X DELETE "http://$TOXIPROXY_HOST/proxies/$service/toxics/latency" 2>/dev/null || true
    curl -sf -X DELETE "http://$TOXIPROXY_HOST/proxies/$service/toxics/bandwidth" 2>/dev/null || true
    curl -sf -X DELETE "http://$TOXIPROXY_HOST/proxies/$service/toxics/connection_cut" 2>/dev/null || true
    curl -sf -X DELETE "http://$TOXIPROXY_HOST/proxies/$service/toxics/slow_close" 2>/dev/null || true

    log_success "Toxics removed"
}

# Monitor system during chaos
monitor_system() {
    local duration=$1
    local service=$2

    log_info "Monitoring system for ${duration}s during chaos..."

    local start_time
    start_time=$(date +%s)
    local metrics_file="/tmp/chaos-metrics-$service-$(date +%s).json"

    echo "[" > "$metrics_file"

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $duration ]]; then
            break
        fi

        # Collect metrics
        local lag
        lag=$(curl -sf 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

        local errors
        errors=$(curl -sf 'http://localhost:9090/api/v1/query?query=rate(cdc_replication_errors_total[1m])' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

        # Append to metrics file
        cat >> "$metrics_file" << EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "elapsed": $elapsed,
  "replication_lag": $lag,
  "error_rate": $errors
},
EOF

        sleep 10
    done

    # Close JSON array
    echo "{}" >> "$metrics_file"
    echo "]" >> "$metrics_file"

    log_success "Monitoring completed. Metrics saved to $metrics_file"
    echo "$metrics_file"
}

# Run chaos scenario
run_scenario() {
    local scenario=$1
    local service=$2
    local intensity=$3
    local duration=$4

    log_info "Running chaos scenario: $scenario on $service"

    # Record baseline metrics
    local baseline_lag
    baseline_lag=$(curl -sf 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

    # Apply toxic
    case $scenario in
        latency)
            add_latency "$service" "$intensity"
            ;;
        bandwidth)
            add_bandwidth_limit "$service" "$intensity"
            ;;
        connection-cut)
            add_connection_cut "$service"
            ;;
        slow-close)
            add_slow_close "$service"
            ;;
        *)
            log_error "Unknown scenario: $scenario"
            return 1
            ;;
    esac

    # Monitor system during chaos
    local metrics_file
    metrics_file=$(monitor_system "$duration" "$service")

    # Remove toxic
    remove_toxics "$service"

    # Wait for recovery
    log_info "Waiting for system recovery..."
    sleep 30

    # Check recovery
    local recovery_lag
    recovery_lag=$(curl -sf 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

    # Generate report
    local report
    report=$(jq -n \
        --arg scenario "$scenario" \
        --arg service "$service" \
        --arg intensity "$intensity" \
        --arg duration "$duration" \
        --arg baseline_lag "$baseline_lag" \
        --arg recovery_lag "$recovery_lag" \
        --arg metrics_file "$metrics_file" \
        '{
            scenario: $scenario,
            service: $service,
            intensity: $intensity,
            duration: ($duration | tonumber),
            baseline_lag: ($baseline_lag | tonumber),
            recovery_lag: ($recovery_lag | tonumber),
            metrics_file: $metrics_file,
            recovery_status: (if ($recovery_lag | tonumber) < (($baseline_lag | tonumber) * 2) then "PASS" else "FAIL" end)
        }')

    echo "$report"
}

# Main function
main() {
    local scenario=""
    local target=""
    local intensity="medium"

    while [[ $# -gt 0 ]]; do
        case $1 in
            --scenario)
                scenario="$2"
                shift 2
                ;;
            --target)
                target="$2"
                shift 2
                ;;
            --duration)
                DURATION="$2"
                shift 2
                ;;
            --intensity)
                intensity="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
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

    if [[ -z "$scenario" ]] || [[ -z "$target" ]]; then
        log_error "Scenario and target are required"
        usage
    fi

    check_toxiproxy

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY RUN] Would run scenario: $scenario on $target"
        exit 0
    fi

    local all_results="[]"

    if [[ "$scenario" == "all" ]]; then
        for test_scenario in latency bandwidth connection-cut slow-close; do
            log_info "Running scenario: $test_scenario"
            local result
            result=$(run_scenario "$test_scenario" "$target" "$intensity" "$DURATION")
            all_results=$(echo "$all_results" | jq --argjson result "$result" '. + [$result]')
        done
    else
        local result
        result=$(run_scenario "$scenario" "$target" "$intensity" "$DURATION")
        all_results=$(echo "$all_results" | jq --argjson result "$result" '. + [$result]')
    fi

    # Save report
    echo "$all_results" | jq '.' > "$REPORT_FILE"
    log_success "Chaos test completed. Report: $REPORT_FILE"

    # Display summary
    echo
    log_info "Test Summary:"
    echo "$all_results" | jq -r '.[] | "  [\(.recovery_status)] \(.scenario) on \(.service) - Recovery lag: \(.recovery_lag)s"'
}

main "$@"
