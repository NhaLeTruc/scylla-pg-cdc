#!/bin/bash
# Export Kafka consumer lag metrics in Prometheus format
# Usage: ./scripts/export-lag-metrics.sh > /tmp/kafka_lag_metrics.prom

set -euo pipefail

# Get consumer group lag
LAG_OUTPUT=$(docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group connect-postgres-jdbc-sink 2>&1 || echo "")

if [ -z "$LAG_OUTPUT" ]; then
  echo "# ERROR: Failed to fetch consumer lag"
  exit 1
fi

# Generate Prometheus metrics
echo "# HELP kafka_consumer_lag Current lag for Kafka consumer group partitions"
echo "# TYPE kafka_consumer_lag gauge"

while IFS= read -r line; do
  if echo "$line" | grep -q "scylla-cluster.app_data"; then
    TOPIC=$(echo "$line" | awk '{print $2}')
    PARTITION=$(echo "$line" | awk '{print $3}')
    LAG=$(echo "$line" | awk '{print $6}')
    
    if [[ "$LAG" =~ ^[0-9]+$ ]]; then
      TABLE=$(echo "$TOPIC" | sed 's/scylla-cluster\.app_data\.//')
      echo "kafka_consumer_lag{group=\"connect-postgres-jdbc-sink\",topic=\"$TOPIC\",table=\"$TABLE\",partition=\"$PARTITION\"} $LAG"
    fi
  fi
done <<< "$LAG_OUTPUT"

echo "# HELP kafka_consumer_lag_total Total lag across all partitions"
echo "# TYPE kafka_consumer_lag_total gauge"

TOTAL_LAG=$(echo "$LAG_OUTPUT" | awk '{sum+=$6} END {print sum+0}')
echo "kafka_consumer_lag_total{group=\"connect-postgres-jdbc-sink\"} $TOTAL_LAG"
