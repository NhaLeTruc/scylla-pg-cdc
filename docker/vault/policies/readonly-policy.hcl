# Vault Read-Only Policy for CDC Pipeline
# Restricted read-only access for monitoring and auditing

# Read-only access to secrets
path "secret/data/*" {
  capabilities = ["read", "list"]
}

path "secret/metadata/*" {
  capabilities = ["list"]
}

# Read system health
path "sys/health" {
  capabilities = ["read"]
}

path "sys/seal-status" {
  capabilities = ["read"]
}

# Read policies
path "sys/policies/acl/*" {
  capabilities = ["read", "list"]
}

# Read token information
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
