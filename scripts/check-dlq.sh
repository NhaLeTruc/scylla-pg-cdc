#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - DLQ Monitor
# ==============================================================================
# Description: Monitor and inspect Dead Letter Queue messages
# Usage: ./scripts/check-dlq.sh [OPTIONS]
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
KAFKA_BROKER="localhost:9092"
DLQ_PATTERN="dlq-*"
MAX_MESSAGES=10
SHOW_CONTENT=false
ALERT_THRESHOLD=100
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

Monitor Dead Letter Queue topics and inspect failed messages.

OPTIONS:
    -h, --help              Show this help message
    -p, --pattern PATTERN   DLQ topic pattern (default: dlq-*)
    -n, --max-messages N    Max messages to show per topic (default: 10)
    -c, --show-content      Show message content
    --alert-threshold N     Alert if DLQ exceeds N messages (default: 100)
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                              # Check all DLQ topics
    $0 --show-content               # Show message details
    $0 --pattern dlq-postgres-*     # Check specific DLQ pattern
    $0 --alert-threshold 50         # Lower alert threshold

DESCRIPTION:
    This script monitors DLQ topics for failed messages:
    - Lists all DLQ topics
    - Shows message counts per topic
    - Displays recent messages (optional)
    - Alerts on high message counts
    - Provides recommendations for remediation
EOF
}

check_prerequisites() {
    if ! docker exec kafka kafka-topics --version &>/dev/null; then
        log_error "Kafka CLI tools not accessible"
        return 1
    fi

    return 0
}

list_dlq_topics() {
    local pattern=$1

    log_verbose "Listing DLQ topics matching pattern: $pattern"

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep "$pattern" || echo ""
}

get_topic_message_count() {
    local topic=$1

    # Get end offsets for all partitions and sum them
    local count
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic "$topic" \
        --time -1 2>/dev/null | \
        awk -F: '{sum += $3} END {print sum+0}')

    echo "$count"
}

get_topic_partitions() {
    local topic=$1

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --describe \
        --topic "$topic" 2>/dev/null | \
        grep "PartitionCount" | \
        awk '{print $4}'
}

read_topic_messages() {
    local topic=$1
    local max_messages=$2

    log_verbose "Reading up to $max_messages messages from $topic"

    timeout 10s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$max_messages" \
        --timeout-ms 5000 \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.partition=true \
        --property print.offset=true 2>/dev/null || true
}

analyze_message_errors() {
    local topic=$1

    log_verbose "Analyzing error patterns in $topic"

    # Read messages and extract error information
    local messages
    messages=$(timeout 5s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages 100 \
        --timeout-ms 3000 2>/dev/null || echo "")

    if [ -z "$messages" ]; then
        log_verbose "No messages to analyze"
        return
    fi

    # Count common error types (simplified analysis)
    local schema_errors
    local timeout_errors
    local validation_errors

    schema_errors=$(echo "$messages" | grep -ci "schema" || echo "0")
    timeout_errors=$(echo "$messages" | grep -ci "timeout" || echo "0")
    validation_errors=$(echo "$messages" | grep -ci "validation\|invalid" || echo "0")

    if [ "$schema_errors" -gt 0 ]; then
        log_warning "  Schema errors detected: $schema_errors"
    fi

    if [ "$timeout_errors" -gt 0 ]; then
        log_warning "  Timeout errors detected: $timeout_errors"
    fi

    if [ "$validation_errors" -gt 0 ]; then
        log_warning "  Validation errors detected: $validation_errors"
    fi
}

get_recommendation() {
    local message_count=$1
    local topic=$2

    if [ "$message_count" -eq 0 ]; then
        echo "No action needed - DLQ is empty"
        return
    fi

    if [ "$message_count" -lt 10 ]; then
        echo "Low message count - monitor and investigate individual failures"
    elif [ "$message_count" -lt 100 ]; then
        echo "Moderate message count - review error patterns and consider fixes"
    else
        echo "High message count - immediate investigation required!"
        echo "    1. Check connector logs for error patterns"
        echo "    2. Review schema compatibility settings"
        echo "    3. Verify data validation rules"
        echo "    4. Consider using scripts/replay-dlq.sh after fixes"
    fi
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
        -p|--pattern)
            DLQ_PATTERN="$2"
            shift 2
            ;;
        -n|--max-messages)
            MAX_MESSAGES="$2"
            shift 2
            ;;
        -c|--show-content)
            SHOW_CONTENT=true
            shift
            ;;
        --alert-threshold)
            ALERT_THRESHOLD="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
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
log_info "Dead Letter Queue Monitor"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  DLQ pattern: $DLQ_PATTERN"
echo "  Alert threshold: $ALERT_THRESHOLD messages"
echo "  Show content: $SHOW_CONTENT"
echo ""

# Check prerequisites
log_verbose "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi

# List DLQ topics
log_info "Scanning for DLQ topics..."
dlq_topics=$(list_dlq_topics "$DLQ_PATTERN")

if [ -z "$dlq_topics" ]; then
    log_success "No DLQ topics found matching pattern: $DLQ_PATTERN"
    log_info "This indicates no messages have been routed to DLQ (good!)"
    exit 0
fi

topic_count=$(echo "$dlq_topics" | wc -l)
log_info "Found $topic_count DLQ topic(s)"
echo ""

# Analyze each DLQ topic
total_messages=0
topics_with_alerts=()

for topic in $dlq_topics; do
    log_info "-------------------------------------------------------------------"
    log_info "Topic: $topic"
    log_info "-------------------------------------------------------------------"

    # Get message count
    message_count=$(get_topic_message_count "$topic")
    total_messages=$((total_messages + message_count))

    # Get partition count
    partition_count=$(get_topic_partitions "$topic")

    echo "  Messages: $message_count"
    echo "  Partitions: $partition_count"

    # Alert if threshold exceeded
    if [ "$message_count" -gt "$ALERT_THRESHOLD" ]; then
        log_error "  ALERT: Message count exceeds threshold ($ALERT_THRESHOLD)"
        topics_with_alerts+=("$topic")
    elif [ "$message_count" -gt 0 ]; then
        log_warning "  Warning: DLQ contains messages"
    else
        log_success "  Empty"
    fi

    # Show messages if requested
    if [ "$SHOW_CONTENT" = true ] && [ "$message_count" -gt 0 ]; then
        echo ""
        log_info "  Recent messages (up to $MAX_MESSAGES):"
        echo "  ..................................................................."

        read_topic_messages "$topic" "$MAX_MESSAGES" | head -20

        echo "  ..................................................................."
        echo ""

        # Analyze error patterns
        log_info "  Error pattern analysis:"
        analyze_message_errors "$topic"
    fi

    # Get recommendation
    echo ""
    log_info "  Recommendation:"
    get_recommendation "$message_count" "$topic" | sed 's/^/    /'

    echo ""
done

# Summary
log_info "==================================================================="
log_info "Summary"
log_info "==================================================================="
echo ""

log_info "Total DLQ topics: $topic_count"
log_info "Total messages in DLQ: $total_messages"

if [ ${#topics_with_alerts[@]} -gt 0 ]; then
    log_error "Topics exceeding alert threshold: ${#topics_with_alerts[@]}"
    for topic in "${topics_with_alerts[@]}"; do
        echo "  • $topic"
    done
else
    log_success "No topics exceeding alert threshold"
fi

echo ""

# Overall status
if [ "$total_messages" -eq 0 ]; then
    log_success "All DLQ topics are empty - pipeline is healthy ✓"
    exit 0
elif [ "$total_messages" -lt "$ALERT_THRESHOLD" ]; then
    log_warning "DLQ contains messages but below critical threshold"
    log_info "Monitor for patterns and investigate individual failures"
    exit 0
else
    log_error "DLQ message count is critically high"
    log_info "Immediate action required:"
    echo "  1. Review connector logs: docker compose logs kafka-connect"
    echo "  2. Check for schema incompatibilities"
    echo "  3. Verify data validation and transformation rules"
    echo "  4. Use --show-content to inspect failed messages"
    echo "  5. Fix root cause before using scripts/replay-dlq.sh"
    exit 1
fi
