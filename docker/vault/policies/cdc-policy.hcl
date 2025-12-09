# Vault Policy for CDC Pipeline Components
# This policy grants access to secrets required by the CDC pipeline

# Allow reading database credentials
path "secret/data/scylla-credentials" {
  capabilities = ["read", "list"]
}

path "secret/data/postgres-credentials" {
  capabilities = ["read", "list"]
}

# Allow reading Kafka credentials
path "secret/data/kafka-credentials" {
  capabilities = ["read", "list"]
}

path "secret/data/schema-registry-credentials" {
  capabilities = ["read", "list"]
}

# Allow reading monitoring credentials
path "secret/data/grafana-credentials" {
  capabilities = ["read", "list"]
}

path "secret/data/prometheus-credentials" {
  capabilities = ["read", "list"]
}

# Allow listing secrets
path "secret/metadata/*" {
  capabilities = ["list"]
}

# Allow reading system health
path "sys/health" {
  capabilities = ["read"]
}

# Allow reading token information
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
