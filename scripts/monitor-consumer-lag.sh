#!/bin/bash
# Monitor Kafka consumer lag for CDC pipeline
# Usage: ./scripts/monitor-consumer-lag.sh [--watch] [--threshold <num>]

set -euo pipefail

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
WATCH_MODE=false
THRESHOLD=5
INTERVAL=5

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --watch)
      WATCH_MODE=true
      shift
      ;;
    --threshold)
      THRESHOLD="$2"
      shift 2
      ;;
    --interval)
      INTERVAL="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--watch] [--threshold <num>] [--interval <seconds>]"
      exit 1
      ;;
  esac
done

# Function to check consumer lag
check_lag() {
  echo -e "${BLUE}=== Kafka Consumer Lag Monitor ===${NC}"
  echo -e "Time: $(date '+%Y-%m-%d %H:%M:%S')"
  echo ""
  
  # Get consumer group lag
  LAG_OUTPUT=$(docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe \
    --group connect-postgres-jdbc-sink 2>&1 || echo "ERROR")
  
  if echo "$LAG_OUTPUT" | grep -q "ERROR"; then
    echo -e "${RED}Failed to fetch consumer lag${NC}"
    return 1
  fi
  
  # Parse and display lag information
  echo "$LAG_OUTPUT" | head -1
  
  TOTAL_LAG=0
  MAX_LAG=0
  LAGGING_PARTITIONS=0
  
  while IFS= read -r line; do
    if echo "$line" | grep -q "scylla-cluster.app_data"; then
      LAG=$(echo "$line" | awk '{print $6}')
      
      if [[ "$LAG" =~ ^[0-9]+$ ]]; then
        TOTAL_LAG=$((TOTAL_LAG + LAG))
        
        if [ "$LAG" -gt "$MAX_LAG" ]; then
          MAX_LAG=$LAG
        fi
        
        if [ "$LAG" -gt "$THRESHOLD" ]; then
          LAGGING_PARTITIONS=$((LAGGING_PARTITIONS + 1))
          echo -e "${YELLOW}$line${NC}"
        elif [ "$LAG" -gt 0 ]; then
          echo -e "${GREEN}$line${NC}"
        else
          echo "$line"
        fi
      fi
    fi
  done <<< "$LAG_OUTPUT"
  
  echo ""
  echo -e "${BLUE}Summary:${NC}"
  echo -e "  Total Lag: ${TOTAL_LAG}"
  echo -e "  Max Lag: ${MAX_LAG}"
  
  if [ "$MAX_LAG" -gt "$THRESHOLD" ]; then
    echo -e "  Status: ${RED}WARNING - High lag detected!${NC}"
    echo -e "  Lagging Partitions: ${LAGGING_PARTITIONS}"
  elif [ "$TOTAL_LAG" -gt 0 ]; then
    echo -e "  Status: ${YELLOW}Lagging (within threshold)${NC}"
  else
    echo -e "  Status: ${GREEN}All caught up${NC}"
  fi
  
  echo ""
}

# Main execution
if [ "$WATCH_MODE" = true ]; then
  echo -e "${BLUE}Starting consumer lag monitor (press Ctrl+C to stop)${NC}"
  echo -e "Refresh interval: ${INTERVAL}s, Lag threshold: ${THRESHOLD}"
  echo ""
  
  while true; do
    clear
    check_lag
    sleep "$INTERVAL"
  done
else
  check_lag
fi
