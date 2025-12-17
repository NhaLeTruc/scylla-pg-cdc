# Kafka Connect Connector Configurations

This directory contains Kafka Connect connector configurations for the ScyllaDB to PostgreSQL CDC pipeline.

## Overview

The connector configurations use a template-based approach where environment variables from `.env` are substituted into templates to generate the final JSON configuration files.

## Files

- `*.json.template` - Template files with environment variable placeholders
- `*.json` - Generated configuration files (auto-generated, not committed to git)

## Template Variables

All connector and topic names are configured via environment variables in the `.env` file:

### Connector Names
- `SOURCE_CONNECTOR_NAME` - Name of the ScyllaDB CDC source connector
- `SINK_CONNECTOR_NAME` - Name of the PostgreSQL JDBC sink connector

### Topic Configuration
- `CDC_TOPIC_PREFIX` - Prefix for CDC data topics
- `CDC_SCYLLA_CLUSTER_NAME` - ScyllaDB cluster name used in topic naming
- `CDC_HEARTBEAT_TOPIC_PREFIX` - Prefix for heartbeat topics
- `CDC_SCHEMA_HISTORY_TOPIC` - Schema history topic name
- `CDC_SOURCE_DLQ_TOPIC` - Dead letter queue topic for source connector
- `SINK_TOPICS_REGEX` - Regex pattern for sink connector topics
- `SINK_DLQ_TOPIC` - Dead letter queue topic for sink connector

### Connection Configuration
- `SCYLLA_HOSTS` - ScyllaDB contact points
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` - PostgreSQL connection details
- `KAFKA_BROKERS` - Kafka bootstrap servers
- `SCHEMA_REGISTRY_URL` - Schema Registry URL

### Performance Configuration
- `CONNECTOR_TASKS_MAX` - Maximum number of tasks per connector
- `KAFKA_REPLICATION_FACTOR` - Topic replication factor
- `KAFKA_NUM_PARTITIONS` - Number of partitions for topics
- `KAFKA_RETENTION_MS` - Topic retention time in milliseconds
- `SINK_BATCH_SIZE` - Batch size for sink connector
- `MAX_RETRIES` - Maximum retry attempts
- `RETRY_BACKOFF_MS` - Retry backoff time in milliseconds

## Usage

### Automatic Processing

The `deploy-connectors.sh` script automatically processes templates before deployment:

```bash
./scripts/deploy-connectors.sh
```

### Manual Processing

To manually generate configuration files from templates:

```bash
# Process source connector template
./scripts/process-connector-config.sh \
  --template docker/kafka-connect/connectors/scylla-source.json.template \
  --output docker/kafka-connect/connectors/scylla-source.json \
  --env-file .env \
  --validate

# Process sink connector template
./scripts/process-connector-config.sh \
  --template docker/kafka-connect/connectors/postgres-sink.json.template \
  --output docker/kafka-connect/connectors/postgres-sink.json \
  --env-file .env \
  --validate
```

## Customization

To customize connector or topic names:

1. Edit the `.env` file with your desired values
2. Run the deployment script, which will automatically regenerate the JSON files
3. The connectors will be deployed with your custom configuration

Example `.env` modifications:

```bash
# Custom connector names
SOURCE_CONNECTOR_NAME=my-scylla-source
SINK_CONNECTOR_NAME=my-postgres-sink

# Custom topic configuration
CDC_TOPIC_PREFIX=my-cdc
CDC_SCYLLA_CLUSTER_NAME=prod-scylla
CDC_SOURCE_DLQ_TOPIC=dlq-my-scylla-source
SINK_DLQ_TOPIC=dlq-my-postgres-sink
```

## Important Notes

- The generated `*.json` files are not committed to git (listed in `.gitignore`)
- Always modify the `.env` file, not the generated JSON files
- The template files (`*.json.template`) are the source of truth and are committed to git
- Environment variable substitution happens at deployment time
