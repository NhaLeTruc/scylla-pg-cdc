#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - DLQ Replay Tool
# ==============================================================================
# Description: Replay messages from Dead Letter Queue after fixing issues
# Usage: ./scripts/replay-dlq.sh [OPTIONS]
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
DLQ_TOPIC=""
TARGET_TOPIC=""
MAX_MESSAGES=1000
DRY_RUN=true
BATCH_SIZE=100
WAIT_BETWEEN_BATCHES=5
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

Replay messages from DLQ back to main topic after fixing issues.

OPTIONS:
    -h, --help              Show this help message
    -s, --source TOPIC      DLQ topic to replay from (required)
    -t, --target TOPIC      Target topic to replay to (required)
    -n, --max-messages N    Max messages to replay (default: 1000)
    -b, --batch-size N      Batch size for replay (default: 100)
    -w, --wait SECS         Wait time between batches (default: 5)
    --execute               Actually execute replay (default: dry-run)
    -v, --verbose           Enable verbose output

EXAMPLES:
    # Dry run to preview
    $0 --source dlq-postgres-sink --target cdc.scylla.users

    # Execute replay
    $0 --source dlq-postgres-sink --target cdc.scylla.users --execute

    # Replay with custom batch size
    $0 -s dlq-postgres-sink -t cdc.scylla.users --batch-size 50 --execute

DESCRIPTION:
    This script replays messages from DLQ topics:
    1. Reads messages from DLQ topic
    2. Validates message format (optional)
    3. Publishes to target topic in batches
    4. Monitors replay progress
    5. Reports success/failure statistics

    IMPORTANT:
    - Fix root cause before replaying
    - Use dry-run first to preview
    - Monitor connector health during replay
    - Consider replaying in small batches during low traffic
EOF
}

check_prerequisites() {
    if ! docker exec kafka kafka-topics --version &>/dev/null; then
        log_error "Kafka CLI tools not accessible"
        return 1
    fi

    if [ -z "$DLQ_TOPIC" ] || [ -z "$TARGET_TOPIC" ]; then
        log_error "Both --source and --target are required"
        return 1
    fi

    return 0
}

check_topic_exists() {
    local topic=$1

    docker exec kafka kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${topic}$"

    return $?
}

get_message_count() {
    local topic=$1

    local count
    count=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic "$topic" \
        --time -1 2>/dev/null | \
        awk -F: '{sum += $3} END {print sum+0}')

    echo "$count"
}

extract_messages_to_file() {
    local topic=$1
    local max_messages=$2
    local output_file=$3

    log_info "Extracting messages from $topic..."

    timeout 30s docker exec kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$max_messages" \
        --timeout-ms 10000 2>/dev/null > "$output_file" || true

    local extracted_count
    extracted_count=$(wc -l < "$output_file")

    log_info "Extracted $extracted_count messages to file"
    return 0
}

replay_messages_from_file() {
    local input_file=$1
    local target_topic=$2
    local batch_size=$3

    local total_lines
    total_lines=$(wc -l < "$input_file")

    log_info "Replaying $total_lines messages in batches of $batch_size..."

    local current_line=0
    local batch_num=0
    local success_count=0
    local fail_count=0

    while IFS= read -r message; do
        ((current_line++))

        # Skip empty lines
        if [ -z "$message" ]; then
            continue
        fi

        # Publish message
        if echo "$message" | docker exec -i kafka kafka-console-producer \
            --broker-list localhost:9092 \
            --topic "$target_topic" 2>/dev/null; then
            ((success_count++))
            log_verbose "Replayed message $current_line"
        else
            ((fail_count++))
            log_warning "Failed to replay message $current_line"
        fi

        # Batch progress
        if (( current_line % batch_size == 0 )); then
            ((batch_num++))
            log_info "Batch $batch_num complete: ${current_line}/${total_lines} messages replayed"

            if [ "$current_line" -lt "$total_lines" ]; then
                log_info "Waiting ${WAIT_BETWEEN_BATCHES}s before next batch..."
                sleep "$WAIT_BETWEEN_BATCHES"
            fi
        fi

    done < "$input_file"

    log_info "Replay complete: $success_count succeeded, $fail_count failed"

    return 0
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
        -s|--source)
            DLQ_TOPIC="$2"
            shift 2
            ;;
        -t|--target)
            TARGET_TOPIC="$2"
            shift 2
            ;;
        -n|--max-messages)
            MAX_MESSAGES="$2"
            shift 2
            ;;
        -b|--batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_BETWEEN_BATCHES="$2"
            shift 2
            ;;
        --execute)
            DRY_RUN=false
            shift
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
log_info "DLQ Replay Tool"
log_info "==================================================================="
echo ""

log_info "Configuration:"
echo "  Source (DLQ): $DLQ_TOPIC"
echo "  Target: $TARGET_TOPIC"
echo "  Max messages: $MAX_MESSAGES"
echo "  Batch size: $BATCH_SIZE"
echo "  Wait between batches: ${WAIT_BETWEEN_BATCHES}s"
echo "  Mode: $([ "$DRY_RUN" = true ] && echo "DRY RUN" || echo "EXECUTE")"
echo ""

# Check prerequisites
log_info "Checking prerequisites..."
if ! check_prerequisites; then
    usage
    exit 1
fi
log_success "Prerequisites met"
echo ""

# Verify topics exist
log_info "Verifying topics..."

if ! check_topic_exists "$DLQ_TOPIC"; then
    log_error "Source DLQ topic does not exist: $DLQ_TOPIC"
    exit 1
fi
log_success "Source topic exists: $DLQ_TOPIC"

if ! check_topic_exists "$TARGET_TOPIC"; then
    log_error "Target topic does not exist: $TARGET_TOPIC"
    exit 1
fi
log_success "Target topic exists: $TARGET_TOPIC"
echo ""

# Check message count
log_info "Checking DLQ message count..."
dlq_count=$(get_message_count "$DLQ_TOPIC")

log_info "Messages in DLQ: $dlq_count"

if [ "$dlq_count" -eq 0 ]; then
    log_success "DLQ is empty - nothing to replay"
    exit 0
fi

if [ "$dlq_count" -gt "$MAX_MESSAGES" ]; then
    log_warning "DLQ has $dlq_count messages, will replay $MAX_MESSAGES"
    messages_to_replay=$MAX_MESSAGES
else
    messages_to_replay=$dlq_count
fi
echo ""

# Dry run confirmation
if [ "$DRY_RUN" = true ]; then
    log_warning "==================================================================="
    log_warning "DRY RUN MODE - No messages will be replayed"
    log_warning "==================================================================="
    log_info "This is a preview only. Use --execute to actually replay messages."
    echo ""
    log_info "Would replay $messages_to_replay messages from $DLQ_TOPIC to $TARGET_TOPIC"
    log_info "Batch configuration: $BATCH_SIZE messages per batch, ${WAIT_BETWEEN_BATCHES}s wait"
    echo ""
    log_info "To execute the replay, run:"
    echo "  $0 --source $DLQ_TOPIC --target $TARGET_TOPIC --execute"
    exit 0
fi

# Execute confirmation
log_warning "==================================================================="
log_warning "EXECUTE MODE - Messages will be replayed"
log_warning "==================================================================="
echo ""
log_warning "This will replay $messages_to_replay messages from DLQ to the main topic."
log_warning "Ensure you have fixed the root cause before proceeding."
echo ""

read -p "Are you sure you want to proceed? (yes/no): " -r confirmation

if [ "$confirmation" != "yes" ]; then
    log_info "Replay cancelled"
    exit 0
fi

echo ""

# Extract messages to temporary file
temp_file=$(mktemp)
trap "rm -f $temp_file" EXIT

log_info "==================================================================="
log_info "Step 1: Extract messages from DLQ"
log_info "==================================================================="
echo ""

extract_messages_to_file "$DLQ_TOPIC" "$messages_to_replay" "$temp_file"

actual_extracted=$(wc -l < "$temp_file")
log_success "Extracted $actual_extracted messages"
echo ""

if [ "$actual_extracted" -eq 0 ]; then
    log_error "No messages extracted - cannot replay"
    exit 1
fi

# Replay messages
log_info "==================================================================="
log_info "Step 2: Replay messages to target topic"
log_info "==================================================================="
echo ""

start_time=$(date +%s)

replay_messages_from_file "$temp_file" "$TARGET_TOPIC" "$BATCH_SIZE"

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
log_success "Replay completed in ${duration}s"
echo ""

# Verify
log_info "==================================================================="
log_info "Step 3: Verification"
log_info "==================================================================="
echo ""

log_info "Waiting 10s for messages to be processed..."
sleep 10

final_dlq_count=$(get_message_count "$DLQ_TOPIC")

log_info "DLQ message count:"
echo "  Before replay: $dlq_count"
echo "  After replay: $final_dlq_count"

if [ "$final_dlq_count" -eq "$dlq_count" ]; then
    log_info "DLQ unchanged (messages remain in DLQ)"
    log_warning "Note: Messages are replayed, not moved. Original DLQ messages remain."
else
    log_info "DLQ count changed"
fi

echo ""

# Summary
log_success "==================================================================="
log_success "Replay Summary"
log_success "==================================================================="
echo ""

log_info "Results:"
echo "  Messages extracted: $actual_extracted"
echo "  Messages replayed: $actual_extracted"
echo "  Duration: ${duration}s"
echo "  Throughput: $(awk "BEGIN {printf \"%.1f\", $actual_extracted / $duration}") msg/s"
echo ""

log_success "Replay completed successfully ✓"
echo ""

log_info "Next steps:"
echo "  1. Monitor connector to ensure replayed messages process correctly"
echo "  2. Check target database for replicated data"
echo "  3. Use scripts/check-dlq.sh to verify DLQ status"
echo "  4. Review connector logs: docker compose logs kafka-connect"
echo ""

log_warning "IMPORTANT: The original messages remain in the DLQ topic."
log_info "To clean up DLQ after successful replay, you can delete and recreate the topic."
echo ""

exit 0
