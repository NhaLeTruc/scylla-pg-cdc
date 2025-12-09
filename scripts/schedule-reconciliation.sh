#!/usr/bin/env bash
# ==============================================================================
# ScyllaDB to Postgres CDC Pipeline - Scheduled Reconciliation Wrapper
# ==============================================================================
# Description: Cron wrapper for automated reconciliation with logging and notifications
# Usage: ./scripts/schedule-reconciliation.sh [OPTIONS]
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
TABLES="users products orders"
MODE="incremental"
LOG_DIR="${PROJECT_ROOT}/logs/reconciliation"
LOG_RETENTION_DAYS=30
ALERT_EMAIL=""
ALERT_ON_FAILURE=true
ALERT_ON_DISCREPANCIES=false
DISCREPANCY_THRESHOLD=1.0  # Alert if > 1% discrepancies
DRY_RUN=false
VERBOSE=false

# ==============================================================================
# Helper Functions
# ==============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1" | tee -a "$LOG_FILE"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Automated reconciliation scheduler with logging and alerts.

OPTIONS:
    -h, --help              Show this help message
    -t, --tables TABLES     Space-separated list of tables (default: users products orders)
    -m, --mode MODE         Reconciliation mode: full or incremental (default: incremental)
    --dry-run               Run in dry-run mode (no actual repairs)
    --email EMAIL           Email address for alerts
    --alert-on-failures     Send email on reconciliation failures (default: true)
    --alert-on-discrepancies Send email when discrepancies exceed threshold
    --threshold PCT         Discrepancy threshold percentage for alerts (default: 1.0)
    --log-dir DIR           Log directory (default: logs/reconciliation)
    --retention DAYS        Log retention in days (default: 30)
    -v, --verbose           Enable verbose output

EXAMPLES:
    # Daily incremental reconciliation
    $0 --tables "users products orders" --mode incremental

    # Weekly full reconciliation
    $0 --tables "users products orders" --mode full

    # Dry run with email alerts
    $0 --dry-run --email admin@example.com --alert-on-discrepancies

CRON SETUP:
    # Daily incremental at 2 AM
    0 2 * * * /path/to/schedule-reconciliation.sh --mode incremental

    # Weekly full on Sundays at 3 AM
    0 3 * * 0 /path/to/schedule-reconciliation.sh --mode full

    # Hourly incremental
    0 * * * * /path/to/schedule-reconciliation.sh --mode incremental --tables users
EOF
}

setup_logging() {
    # Create log directory
    mkdir -p "$LOG_DIR"

    # Set log file with timestamp
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    LOG_FILE="${LOG_DIR}/reconciliation_${timestamp}.log"

    log_info "==================================================================="
    log_info "Scheduled Reconciliation - $(date)"
    log_info "==================================================================="
    log_info "Mode: $MODE"
    log_info "Tables: $TABLES"
    log_info "Dry run: $DRY_RUN"
    log_info "Log file: $LOG_FILE"
    echo ""
}

cleanup_old_logs() {
    log_info "Cleaning up logs older than ${LOG_RETENTION_DAYS} days..."

    local deleted_count=0
    while IFS= read -r -d '' old_log; do
        rm -f "$old_log"
        ((deleted_count++))
    done < <(find "$LOG_DIR" -name "reconciliation_*.log" -type f -mtime +${LOG_RETENTION_DAYS} -print0 2>/dev/null)

    if [ $deleted_count -gt 0 ]; then
        log_info "Deleted $deleted_count old log file(s)"
    fi
}

send_alert() {
    local subject=$1
    local message=$2

    if [ -z "$ALERT_EMAIL" ]; then
        log_warning "No alert email configured, skipping notification"
        return
    fi

    log_info "Sending alert email to: $ALERT_EMAIL"

    # Try to send email using available methods
    if command -v mail &>/dev/null; then
        echo "$message" | mail -s "$subject" "$ALERT_EMAIL"
    elif command -v sendmail &>/dev/null; then
        echo -e "Subject: $subject\n\n$message" | sendmail "$ALERT_EMAIL"
    else
        log_warning "No mail command available, alert not sent"
        log_info "Alert message: $message"
    fi
}

reconcile_table() {
    local table=$1

    log_info "-------------------------------------------------------------------"
    log_info "Reconciling table: $table"
    log_info "-------------------------------------------------------------------"

    local reconcile_cmd="${SCRIPT_DIR}/reconcile.py reconcile"
    reconcile_cmd="$reconcile_cmd --table $table"
    reconcile_cmd="$reconcile_cmd --mode $MODE"

    if [ "$DRY_RUN" = true ]; then
        reconcile_cmd="$reconcile_cmd --dry-run"
    fi

    if [ "$VERBOSE" = true ]; then
        reconcile_cmd="$reconcile_cmd --verbose"
    fi

    log_info "Executing: $reconcile_cmd"

    local output
    local exit_code=0

    output=$(python3 $reconcile_cmd 2>&1) || exit_code=$?

    echo "$output" >> "$LOG_FILE"

    if [ $exit_code -ne 0 ]; then
        log_error "Reconciliation failed for table: $table (exit code: $exit_code)"

        if [ "$ALERT_ON_FAILURE" = true ]; then
            send_alert \
                "Reconciliation Failed: $table" \
                "Reconciliation failed for table $table.\n\nError:\n$output\n\nLog file: $LOG_FILE"
        fi

        return 1
    fi

    # Parse results
    local missing_count
    local extra_count
    local mismatch_count
    local accuracy

    missing_count=$(echo "$output" | jq -r '.discrepancies.missing_count // 0' 2>/dev/null || echo "0")
    extra_count=$(echo "$output" | jq -r '.discrepancies.extra_count // 0' 2>/dev/null || echo "0")
    mismatch_count=$(echo "$output" | jq -r '.discrepancies.mismatch_count // 0' 2>/dev/null || echo "0")

    log_success "Reconciliation completed for $table"
    log_info "  Missing: $missing_count, Extra: $extra_count, Mismatches: $mismatch_count"

    # Check if discrepancies exceed threshold
    local total_source
    total_source=$(echo "$output" | jq -r '.summary.total_source_rows // 0' 2>/dev/null || echo "0")

    if [ "$total_source" -gt 0 ]; then
        local total_discrepancies=$((missing_count + mismatch_count))
        local discrepancy_pct=$(awk "BEGIN {printf \"%.2f\", ($total_discrepancies / $total_source) * 100}")

        log_info "  Accuracy: $(awk "BEGIN {printf \"%.2f\", 100 - $discrepancy_pct}")%"

        # Alert if threshold exceeded
        if [ "$ALERT_ON_DISCREPANCIES" = true ]; then
            if (( $(echo "$discrepancy_pct > $DISCREPANCY_THRESHOLD" | bc -l) )); then
                log_warning "Discrepancy threshold exceeded: ${discrepancy_pct}% > ${DISCREPANCY_THRESHOLD}%"

                send_alert \
                    "Reconciliation Alert: High Discrepancies in $table" \
                    "Table $table has ${discrepancy_pct}% discrepancies (threshold: ${DISCREPANCY_THRESHOLD}%).\n\nMissing: $missing_count\nExtra: $extra_count\nMismatches: $mismatch_count\n\nLog file: $LOG_FILE"
            fi
        fi
    fi

    echo ""
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
        -t|--tables)
            TABLES="$2"
            shift 2
            ;;
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --email)
            ALERT_EMAIL="$2"
            shift 2
            ;;
        --alert-on-failures)
            ALERT_ON_FAILURE=true
            shift
            ;;
        --alert-on-discrepancies)
            ALERT_ON_DISCREPANCIES=true
            shift
            ;;
        --threshold)
            DISCREPANCY_THRESHOLD="$2"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --retention)
            LOG_RETENTION_DAYS="$2"
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

# Setup logging
setup_logging

# Cleanup old logs
cleanup_old_logs

# Run reconciliation for each table
failed_tables=()
success_count=0

for table in $TABLES; do
    if reconcile_table "$table"; then
        ((success_count++))
    else
        failed_tables+=("$table")
    fi
done

# Summary
log_info "==================================================================="
log_info "Reconciliation Summary"
log_info "==================================================================="
log_info "Total tables: $(echo "$TABLES" | wc -w)"
log_info "Successful: $success_count"
log_info "Failed: ${#failed_tables[@]}"

if [ ${#failed_tables[@]} -gt 0 ]; then
    log_error "Failed tables: ${failed_tables[*]}"
    log_info "Check log file for details: $LOG_FILE"
    exit 1
else
    log_success "All reconciliations completed successfully"
fi

log_info "Log file: $LOG_FILE"
echo ""

exit 0
