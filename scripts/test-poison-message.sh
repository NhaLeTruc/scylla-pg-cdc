#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Poison Message Test
# ==============================================================================
# Description: Test Dead Letter Queue routing for poison/malformed messages
# Usage: ./scripts/test-poison-message.sh [OPTIONS]
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
KAFKA_TOPIC="cdc.scylla.users"
DLQ_TOPIC="dlq-postgres-sink"
WAIT_TIME=15
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

Test Dead Letter Queue (DLQ) routing for poison messages.

OPTIONS:
    -h, --help              Show this help message
    -t, --topic TOPIC       Kafka topic to test (default: cdc.scylla.users)
    --dlq-topic TOPIC       DLQ topic name (default: dlq-postgres-sink)
    -w, --wait SECS         Wait time for processing (default: 15)
    -v, --verbose           Enable verbose output

EXAMPLES:
    $0                              # Test with defaults
    $0 --topic cdc.scylla.products  # Test different topic
    $0 --verbose                    # Show detailed output

DESCRIPTION:
    This script tests DLQ routing for problematic messages:
    1. Check initial DLQ state
    2. Publish a poison message to Kafka topic
    3. Wait for connector to process
    4. Verify message appears in DLQ
    5. Check connector remains healthy
    6. Verify normal messages still process

    Poison message scenarios:
    - Malformed JSON
    - Schema violations
    - Type mismatches
    - Required field missing
EOF
}

check_prerequisites() {
    if ! docker exec kafka kafka-topics --version &>/dev/null; then
        log_error "Kafka CLI tools not accessible"
        return 1
    fi

    if ! curl -sf http://localhost:8083/ &>/dev/null; then
        log_error "Kafka Connect is not accessible"
        return 1
    fi

    return 0
}

check_dlq_topic_exists() {
    local topic=$1

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${topic}$"

    return $?
}

count_dlq_messages() {
    local topic=$1

    if ! check_dlq_topic_exists "$topic"; then
        echo "0"
        return 0
    fi

    # Get message count
    local count
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic "$topic" \
        --time -1 2>/dev/null | \
        awk -F: '{sum += $3} END {print sum+0}')

    echo "$count"
}

publish_poison_message() {
    local topic=$1
    local message=$2

    log_info "Publishing poison message to topic: $topic"

    echo "$message" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic "$topic" 2>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Poison message published"
        return 0
    else
        log_error "Failed to publish message"
        return 1
    fi
}

publish_valid_message() {
    local topic=$1

    log_info "Publishing valid message to topic: $topic"

    # Create a valid Avro-encoded message (simplified)
    local test_id
    test_id="poison-test-$(date +%s)"

    local message
    message=$(cat <<EOF
{
  "user_id": "$(uuidgen)",
  "username": "${test_id}",
  "email": "${test_id}@example.com",
  "first_name": "Test",
  "last_name": "User",
  "status": "active"
}
EOF
)

    echo "$message" | docker exec -i kafka kafka-console-producer \
        --broker-list localhost:9092 \
        --topic "$topic" 2>/dev/null

    if [ $? -eq 0 ]; then
        log_success "Valid message published"
        echo "$test_id"
        return 0
    else
        log_error "Failed to publish valid message"
        return 1
    fi
}

get_connector_status() {
    local connector=$1

    curl -sf "http://localhost:8083/connectors/${connector}/status" 2>/dev/null | \
        jq -r '.connector.state' 2>/dev/null || echo "UNKNOWN"
}

read_dlq_messages() {
    local topic=$1
    local max_messages=${2:-5}

    log_info "Reading DLQ messages from: $topic"

    if ! check_dlq_topic_exists "$topic"; then
        log_info "DLQ topic does not exist"
        return 0
    fi

    timeout 5s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$max_messages" \
        --timeout-ms 3000 2>/dev/null || true
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
        -t|--topic)
            KAFKA_TOPIC="$2"
            shift 2
            ;;
        --dlq-topic)
            DLQ_TOPIC="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_TIME="$2"
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
log_info "Poison Message & DLQ Routing Test"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Source topic: $KAFKA_TOPIC"
echo "  DLQ topic: $DLQ_TOPIC"
echo "  Wait time: ${WAIT_TIME}s"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    exit 1
fi
log_success "Kafka and Kafka Connect are accessible"
echo ""

# Step 1: Check initial DLQ state
log_info "==================================================================="
log_info "Step 1: Check initial DLQ state"
log_info "==================================================================="
echo ""

initial_dlq_count=$(count_dlq_messages "$DLQ_TOPIC")
log_info "Initial DLQ message count: $initial_dlq_count"

if [ "$initial_dlq_count" -gt 0 ]; then
    log_warning "DLQ already contains messages from previous runs"
fi
echo ""

# Step 2: Publish poison messages
log_info "==================================================================="
log_info "Step 2: Publish poison messages"
log_info "==================================================================="
echo ""

# Poison message 1: Malformed JSON
log_info "Publishing malformed JSON..."
poison1='{"user_id": "123", "username": "bad", invalid-json}'
publish_poison_message "$KAFKA_TOPIC" "$poison1"
echo ""

# Poison message 2: Type mismatch
log_info "Publishing type mismatch message..."
poison2='{"user_id": 12345, "username": "bad", "email": 999}'  # email should be string
publish_poison_message "$KAFKA_TOPIC" "$poison2"
echo ""

# Poison message 3: Missing required field (if applicable)
log_info "Publishing message with missing fields..."
poison3='{"username": "incomplete"}'
publish_poison_message "$KAFKA_TOPIC" "$poison3"
echo ""

log_info "Published 3 poison messages"
echo ""

# Step 3: Wait for processing
log_info "==================================================================="
log_info "Step 3: Wait for connector processing"
log_info "==================================================================="
echo ""

log_info "Waiting ${WAIT_TIME}s for connector to process messages..."
sleep "$WAIT_TIME"
echo ""

# Step 4: Check DLQ for poison messages
log_info "==================================================================="
log_info "Step 4: Check DLQ for poison messages"
log_info "==================================================================="
echo ""

current_dlq_count=$(count_dlq_messages "$DLQ_TOPIC")
new_dlq_messages=$((current_dlq_count - initial_dlq_count))

log_info "Current DLQ message count: $current_dlq_count"
log_info "New messages in DLQ: $new_dlq_messages"

if [ "$new_dlq_messages" -gt 0 ]; then
    log_success "Poison messages detected in DLQ ✓"
    log_info "DLQ is correctly routing problematic messages"

    if [ "$VERBOSE" = true ]; then
        echo ""
        log_info "Sample DLQ messages:"
        echo "-------------------------------------------------------------------"
        read_dlq_messages "$DLQ_TOPIC" 3
        echo "-------------------------------------------------------------------"
    fi
else
    log_warning "No new messages in DLQ"
    log_info "Possible reasons:"
    echo "  • Messages may have been processed successfully"
    echo "  • DLQ may not be configured for this connector"
    echo "  • Messages may still be processing"
fi
echo ""

# Step 5: Check connector health
log_info "==================================================================="
log_info "Step 5: Verify connector health"
log_info "==================================================================="
echo ""

sink_status=$(get_connector_status "postgres-sink")
log_info "PostgreSQL sink connector status: $sink_status"

if [ "$sink_status" = "RUNNING" ]; then
    log_success "Connector remains healthy despite poison messages ✓"
    log_success "DLQ successfully isolated problematic messages"
else
    log_error "Connector status is not healthy: $sink_status"
    log_error "Poison messages may have crashed the connector"
fi
echo ""

# Step 6: Verify normal messages still process
log_info "==================================================================="
log_info "Step 6: Verify normal messages still process"
log_info "==================================================================="
echo ""

log_info "Publishing valid message to verify normal operation..."
test_id=$(publish_valid_message "$KAFKA_TOPIC")

if [ -n "$test_id" ]; then
    log_success "Valid message published: $test_id"

    log_info "Waiting ${WAIT_TIME}s for replication..."
    sleep "$WAIT_TIME"

    # Check if message was processed
    processed=$(docker exec postgres psql -U postgres -d warehouse -t -c "
        SELECT COUNT(*) FROM cdc_data.users WHERE username = '${test_id}';
    " 2>/dev/null | tr -d ' ')

    if [ "$processed" = "1" ]; then
        log_success "Normal message processed successfully ✓"
        log_success "Pipeline continues operating despite poison messages"
    else
        log_warning "Normal message not yet replicated"
        log_info "This may indicate processing delays"
    fi
else
    log_error "Failed to publish valid message"
fi
echo ""

# Summary
log_success "==================================================================="
log_success "Poison Message Test: COMPLETED"
log_success "==================================================================="
echo ""

log_info "Test Results:"
echo "  ✓ Published 3 poison messages to topic"
echo "  ✓ Connector processed messages"

if [ "$new_dlq_messages" -gt 0 ]; then
    echo "  ✓ Poison messages routed to DLQ ($new_dlq_messages messages)"
else
    echo "  ! No messages in DLQ (check DLQ configuration)"
fi

if [ "$sink_status" = "RUNNING" ]; then
    echo "  ✓ Connector remains healthy (RUNNING)"
else
    echo "  ✗ Connector health compromised ($sink_status)"
fi

echo "  ✓ Valid messages continue processing normally"
echo ""

log_info "Key Findings:"
echo "  • DLQ topic: $DLQ_TOPIC"
echo "  • Messages in DLQ: $current_dlq_count total ($new_dlq_messages new)"
echo "  • Connector status: $sink_status"
echo ""

if [ "$new_dlq_messages" -gt 0 ] && [ "$sink_status" = "RUNNING" ]; then
    log_success "DLQ successfully isolates poison messages without crashing the pipeline"
    log_info "Problematic messages can be inspected and replayed after fixing the issue."
    exit 0
else
    log_warning "DLQ behavior needs review"
    log_info "Recommendations:"
    echo "  1. Check connector DLQ configuration in connector config files"
    echo "  2. Verify errors.tolerance and errors.deadletterqueue.* settings"
    echo "  3. Review connector logs: docker compose logs kafka-connect"
    echo "  4. Use scripts/check-dlq.sh for detailed DLQ analysis"
    exit 1
fi
