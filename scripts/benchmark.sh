#!/usr/bin/env bash

#
# Performance Benchmarking Script for CDC Pipeline
#
# Measures throughput, latency, and resource utilization
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
DURATION="${DURATION:-300}"
PROFILE="${PROFILE:-medium}"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp}"
COMPARE_BASELINE="${COMPARE_BASELINE:-}"

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Performance benchmarking for CDC pipeline.

OPTIONS:
    --profile PROFILE       Benchmark profile: small|medium|large|stress (default: medium)
    --duration SECONDS      Test duration (default: 300)
    --output-dir DIR        Output directory (default: /tmp)
    --compare FILE          Compare against baseline file
    -h, --help              Show this help

PROFILES:
    small       1,000 ops/sec for 5 minutes
    medium      10,000 ops/sec for 5 minutes
    large       50,000 ops/sec for 10 minutes
    stress      Maximum throughput until failure

EXAMPLES:
    # Run medium benchmark
    $0 --profile medium

    # Run stress test
    $0 --profile stress --duration 600

    # Compare against baseline
    $0 --profile medium --compare /tmp/baseline.json

EOF
    exit 0
}

# Get profile configuration
get_profile_config() {
    local profile=$1

    case $profile in
        small)
            echo '{"rate": 1000, "duration": 300, "workers": 2, "batch_size": 100}'
            ;;
        medium)
            echo '{"rate": 10000, "duration": 300, "workers": 10, "batch_size": 1000}'
            ;;
        large)
            echo '{"rate": 50000, "duration": 600, "workers": 20, "batch_size": 5000}'
            ;;
        stress)
            echo '{"rate": 999999, "duration": 600, "workers": 50, "batch_size": 10000}'
            ;;
        *)
            log_error "Unknown profile: $profile"
            exit 1
            ;;
    esac
}

# Record baseline metrics
record_baseline() {
    log_info "Recording baseline metrics..."

    local cpu_usage
    cpu_usage=$(docker stats --no-stream --format "{{.CPUPerc}}" kafka-connect | sed 's/%//')

    local mem_usage
    mem_usage=$(docker stats --no-stream --format "{{.MemPerc}}" kafka-connect | sed 's/%//')

    local lag
    lag=$(curl -sf 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

    jq -n \
        --arg cpu "$cpu_usage" \
        --arg mem "$mem_usage" \
        --arg lag "$lag" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{
            timestamp: $timestamp,
            cpu_percent: ($cpu | tonumber),
            memory_percent: ($mem | tonumber),
            replication_lag: ($lag | tonumber)
        }'
}

# Run load generation
run_load_generation() {
    local rate=$1
    local duration=$2
    local workers=$3

    log_info "Starting load generation: $rate ops/sec for ${duration}s with $workers workers..."

    local load_start
    load_start=$(date +%s)

    # Run load generator
    ./scripts/generate-load.sh \
        --rate "$rate" \
        --duration "$duration" \
        --workers "$workers" \
        --operation-mix "insert:60,update:30,delete:10" \
        > /dev/null 2>&1 &

    local load_pid=$!

    # Monitor progress
    while kill -0 $load_pid 2>/dev/null; do
        local elapsed=$(($(date +%s) - load_start))
        local remaining=$((duration - elapsed))

        if [[ $remaining -le 0 ]]; then
            break
        fi

        printf "\r  Progress: %d/%d seconds (%d%%)  " "$elapsed" "$duration" "$((elapsed * 100 / duration))"
        sleep 5
    done

    echo
    wait $load_pid || true

    log_success "Load generation completed"
}

# Collect metrics during test
collect_metrics() {
    local duration=$1
    local output_file=$2

    log_info "Collecting metrics for ${duration}s..."

    local start_time
    start_time=$(date +%s)

    echo "[" > "$output_file"

    local first=true

    while true; do
        local current_time
        current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $duration ]]; then
            break
        fi

        # Collect metrics
        local throughput
        throughput=$(curl -sf 'http://localhost:9090/api/v1/query?query=rate(cdc_records_replicated_total[1m])' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

        local lag
        lag=$(curl -sf 'http://localhost:9090/api/v1/query?query=cdc_replication_lag_seconds' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

        local errors
        errors=$(curl -sf 'http://localhost:9090/api/v1/query?query=rate(cdc_replication_errors_total[1m])' | jq -r '.data.result[0].value[1]' 2>/dev/null || echo "0")

        local cpu
        cpu=$(docker stats --no-stream --format "{{.CPUPerc}}" kafka-connect | sed 's/%//' || echo "0")

        local mem
        mem=$(docker stats --no-stream --format "{{.MemPerc}}" kafka-connect | sed 's/%//' || echo "0")

        # Append to file
        if [[ "$first" != "true" ]]; then
            echo "," >> "$output_file"
        fi
        first=false

        cat >> "$output_file" << EOF
{
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "elapsed": $elapsed,
  "throughput": $(printf "%.2f" "$throughput"),
  "replication_lag": $(printf "%.2f" "$lag"),
  "error_rate": $(printf "%.6f" "$errors"),
  "cpu_percent": $(printf "%.2f" "$cpu"),
  "memory_percent": $(printf "%.2f" "$mem")
}
EOF

        sleep 10
    done

    echo "]" >> "$output_file"

    log_success "Metrics collected: $output_file"
}

# Calculate statistics
calculate_stats() {
    local metrics_file=$1

    log_info "Calculating statistics..."

    jq -r '
        map({
            throughput: .throughput,
            lag: .replication_lag,
            cpu: .cpu_percent,
            mem: .memory_percent
        }) |
        {
            throughput: {
                min: (map(.throughput) | min),
                max: (map(.throughput) | max),
                avg: (map(.throughput) | add / length),
                p50: (map(.throughput) | sort | .[length/2]),
                p95: (map(.throughput) | sort | .[length*0.95 | floor]),
                p99: (map(.throughput) | sort | .[length*0.99 | floor])
            },
            lag: {
                min: (map(.lag) | min),
                max: (map(.lag) | max),
                avg: (map(.lag) | add / length),
                p95: (map(.lag) | sort | .[length*0.95 | floor])
            },
            cpu: {
                min: (map(.cpu) | min),
                max: (map(.cpu) | max),
                avg: (map(.cpu) | add / length)
            },
            memory: {
                min: (map(.mem) | min),
                max: (map(.mem) | max),
                avg: (map(.mem) | add / length)
            }
        }
    ' "$metrics_file"
}

# Generate benchmark report
generate_report() {
    local profile=$1
    local baseline=$2
    local metrics_file=$3
    local stats=$4
    local report_file=$5

    log_info "Generating benchmark report..."

    jq -n \
        --arg profile "$profile" \
        --argjson baseline "$baseline" \
        --argjson stats "$stats" \
        --arg metrics_file "$metrics_file" \
        --arg timestamp "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        '{
            benchmark_timestamp: $timestamp,
            profile: $profile,
            baseline: $baseline,
            statistics: $stats,
            metrics_file: $metrics_file
        }' > "$report_file"

    log_success "Report generated: $report_file"
}

# Compare with baseline
compare_with_baseline() {
    local current_stats=$1
    local baseline_file=$2

    if [[ ! -f "$baseline_file" ]]; then
        log_warn "Baseline file not found: $baseline_file"
        return
    fi

    log_info "Comparing with baseline..."

    local baseline_stats
    baseline_stats=$(jq '.statistics' "$baseline_file")

    local comparison
    comparison=$(jq -n \
        --argjson current "$current_stats" \
        --argjson baseline "$baseline_stats" \
        '{
            throughput_delta: (($current.throughput.avg / $baseline.throughput.avg - 1) * 100),
            lag_delta: (($current.lag.avg / $baseline.lag.avg - 1) * 100),
            cpu_delta: (($current.cpu.avg / $baseline.cpu.avg - 1) * 100)
        }')

    echo
    log_info "Comparison vs. Baseline:"
    echo "$comparison" | jq -r '
        "  Throughput: \(.throughput_delta | . * 100 | floor / 100)% change",
        "  Lag: \(.lag_delta | . * 100 | floor / 100)% change",
        "  CPU: \(.cpu_delta | . * 100 | floor / 100)% change"
    '
}

# Display results
display_results() {
    local stats=$1

    echo
    log_info "Benchmark Results:"
    echo

    echo "$stats" | jq -r '
        "Throughput (records/sec):",
        "  Average: \(.throughput.avg | . * 100 | floor / 100)",
        "  Min: \(.throughput.min | . * 100 | floor / 100)",
        "  Max: \(.throughput.max | . * 100 | floor / 100)",
        "  p95: \(.throughput.p95 | . * 100 | floor / 100)",
        "",
        "Replication Lag (seconds):",
        "  Average: \(.lag.avg | . * 100 | floor / 100)",
        "  Min: \(.lag.min | . * 100 | floor / 100)",
        "  Max: \(.lag.max | . * 100 | floor / 100)",
        "  p95: \(.lag.p95 | . * 100 | floor / 100)",
        "",
        "CPU Utilization (%):",
        "  Average: \(.cpu.avg | . * 100 | floor / 100)",
        "  Min: \(.cpu.min | . * 100 | floor / 100)",
        "  Max: \(.cpu.max | . * 100 | floor / 100)",
        "",
        "Memory Utilization (%):",
        "  Average: \(.memory.avg | . * 100 | floor / 100)",
        "  Min: \(.memory.min | . * 100 | floor / 100)",
        "  Max: \(.memory.max | . * 100 | floor / 100)"
    '
}

# Main function
main() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --profile)
                PROFILE="$2"
                shift 2
                ;;
            --duration)
                DURATION="$2"
                shift 2
                ;;
            --output-dir)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --compare)
                COMPARE_BASELINE="$2"
                shift 2
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

    log_info "Starting benchmark with profile: $PROFILE"

    # Get profile configuration
    local config
    config=$(get_profile_config "$PROFILE")

    local rate
    rate=$(echo "$config" | jq -r '.rate')

    local workers
    workers=$(echo "$config" | jq -r '.workers')

    # Prepare output files
    local timestamp
    timestamp=$(date +%Y%m%d-%H%M%S)
    local metrics_file="$OUTPUT_DIR/benchmark-metrics-$PROFILE-$timestamp.json"
    local report_file="$OUTPUT_DIR/benchmark-report-$PROFILE-$timestamp.json"

    # Record baseline
    local baseline
    baseline=$(record_baseline)

    # Start metrics collection in background
    collect_metrics "$DURATION" "$metrics_file" &
    local metrics_pid=$!

    # Run load generation
    sleep 2
    run_load_generation "$rate" "$DURATION" "$workers"

    # Wait for metrics collection to complete
    wait $metrics_pid || true

    # Calculate statistics
    local stats
    stats=$(calculate_stats "$metrics_file")

    # Generate report
    generate_report "$PROFILE" "$baseline" "$metrics_file" "$stats" "$report_file"

    # Display results
    display_results "$stats"

    # Compare with baseline if provided
    if [[ -n "$COMPARE_BASELINE" ]]; then
        compare_with_baseline "$stats" "$COMPARE_BASELINE"
    fi

    echo
    log_success "Benchmark completed!"
    log_info "Report: $report_file"
    log_info "Metrics: $metrics_file"
}

main "$@"
