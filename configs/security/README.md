# Security Configuration

This directory contains security configuration files for the CDC pipeline.

## Overview

The CDC pipeline implements multiple layers of security:

1. **TLS/SSL Encryption** - In-transit encryption for all connections
2. **Authentication** - Username/password and certificate-based auth
3. **Authorization** - Role-based access control (RBAC)
4. **Secrets Management** - External secrets via HashiCorp Vault
5. **Network Security** - Firewall rules and network segmentation

## Configuration Files

### TLS/SSL Certificates

- `tls/ca.crt` - Certificate Authority certificate
- `tls/server.crt` - Server certificate
- `tls/server.key` - Server private key
- `tls/client.crt` - Client certificate
- `tls/client.key` - Client private key

### Secrets Management

- `vault/` - HashiCorp Vault configuration
- `vault/secrets/` - Mounted secrets directory

### Authentication

- `auth/scylla-auth.yaml` - ScyllaDB authentication configuration
- `auth/postgres-auth.conf` - PostgreSQL authentication rules (pg_hba.conf)
- `auth/kafka-jaas.conf` - Kafka JAAS configuration for SASL

## Quick Start

### 1. Generate TLS Certificates

```bash
# Generate CA certificate
./scripts/generate-certs.sh ca

# Generate server certificates
./scripts/generate-certs.sh server scylla
./scripts/generate-certs.sh server postgres
./scripts/generate-certs.sh server kafka
./scripts/generate-certs.sh server schema-registry

# Generate client certificates
./scripts/generate-certs.sh client kafka-connect
./scripts/generate-certs.sh client reconciliation-service
```

### 2. Configure Vault

```bash
# Initialize Vault
docker-compose up -d vault
export VAULT_ADDR='http://localhost:8200'
vault operator init
vault operator unseal

# Store credentials
vault kv put secret/scylla-credentials username=scylla_user password=<password>
vault kv put secret/postgres-credentials username=postgres_user password=<password>
vault kv put secret/kafka-credentials username=kafka_user password=<password>
```

### 3. Enable TLS in Services

```bash
# ScyllaDB
docker-compose -f docker-compose.yml -f docker-compose.security.yml up -d scylla

# PostgreSQL
docker-compose -f docker-compose.yml -f docker-compose.security.yml up -d postgres

# Kafka
docker-compose -f docker-compose.yml -f docker-compose.security.yml up -d kafka
```

## Security Best Practices

### Secrets Management

1. **Never commit secrets** to version control
   - Use `.gitignore` for `*.key`, `*.pem`, `secrets/`
   - Use environment variables or external secret stores

2. **Rotate credentials regularly**
   - Database passwords: Every 90 days
   - TLS certificates: Every 365 days
   - API keys: Every 180 days

3. **Use strong passwords**
   - Minimum 16 characters
   - Mix of uppercase, lowercase, numbers, symbols
   - Generated via password manager

4. **Principle of least privilege**
   - Grant minimum required permissions
   - Use separate accounts for each service
   - Restrict network access via firewall rules

### TLS/SSL Configuration

1. **Use TLS 1.2 or higher**
   - Disable SSLv3, TLS 1.0, TLS 1.1
   - Use strong cipher suites only

2. **Certificate validation**
   - Verify server certificates
   - Use certificate pinning where possible
   - Check certificate expiration dates

3. **Mutual TLS (mTLS)**
   - Client certificate authentication
   - Both client and server verify each other

### Network Security

1. **Firewall rules**
   ```bash
   # Allow only necessary ports
   # ScyllaDB: 9042 (CQL)
   # PostgreSQL: 5432
   # Kafka: 9092, 9093 (SSL)
   # Schema Registry: 8081
   # Kafka Connect: 8083
   # Prometheus: 9090
   # Grafana: 3000
   ```

2. **Network segmentation**
   - Separate networks for data, management, monitoring
   - Use VPCs/subnets in cloud environments
   - Internal-only access for databases

3. **IP whitelisting**
   - Restrict access to known IP ranges
   - Use VPN for remote access

### Authentication & Authorization

#### ScyllaDB

```yaml
# Enable authentication
authenticator: PasswordAuthenticator
authorizer: CassandraAuthorizer

# Create users
CREATE ROLE scylla_admin WITH PASSWORD = '...' AND SUPERUSER = true AND LOGIN = true;
CREATE ROLE scylla_user WITH PASSWORD = '...' AND LOGIN = true;
GRANT SELECT, MODIFY ON KEYSPACE app_data TO scylla_user;
```

#### PostgreSQL

```conf
# pg_hba.conf
# Require SSL for all connections
hostssl all all 0.0.0.0/0 md5

# Require client certificates
hostssl all all 0.0.0.0/0 cert

# Local connections
local all postgres peer
```

#### Kafka

```properties
# SASL/SCRAM authentication
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512

# SSL encryption
ssl.enabled.protocols=TLSv1.2,TLSv1.3
ssl.cipher.suites=TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,...
```

### Audit Logging

1. **Enable audit logs** for all services
   ```bash
   # ScyllaDB
   audit_logging_options:
     enabled: true
     included_keyspaces: "app_data"

   # PostgreSQL
   log_statement = 'ddl'
   log_connections = on
   log_disconnections = on

   # Kafka
   authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer
   ```

2. **Centralized log collection**
   - Ship logs to SIEM (Splunk, ELK, etc.)
   - Set up alerts for suspicious activity
   - Retain logs for compliance (typically 90-365 days)

### Compliance

#### GDPR / Data Privacy

- **Data encryption**: At rest and in transit
- **Data minimization**: Store only necessary data
- **Access logs**: Track who accessed what data
- **Right to deletion**: Implement data deletion procedures

#### SOC 2 / ISO 27001

- **Access controls**: RBAC, MFA for privileged access
- **Change management**: All changes reviewed and approved
- **Incident response**: Documented procedures
- **Disaster recovery**: Tested backup and restore procedures

## Security Checklist

### Development Environment

- [ ] Secrets loaded from environment variables
- [ ] TLS enabled for all connections
- [ ] Audit logging enabled
- [ ] Vulnerability scanning for dependencies
- [ ] Security headers configured

### Production Environment

- [ ] All secrets stored in Vault or AWS Secrets Manager
- [ ] TLS 1.2+ with strong ciphers
- [ ] Mutual TLS for service-to-service communication
- [ ] Network segmentation via VPCs/subnets
- [ ] Firewall rules with IP whitelisting
- [ ] Role-based access control configured
- [ ] Audit logs shipped to SIEM
- [ ] Intrusion detection system (IDS) enabled
- [ ] Regular security scanning and patching
- [ ] Incident response plan documented
- [ ] Disaster recovery tested quarterly

## Certificate Management

### Generate Self-Signed Certificates (Development Only)

```bash
# CA certificate
openssl genrsa -out configs/security/tls/ca.key 4096
openssl req -new -x509 -days 3650 -key configs/security/tls/ca.key -out configs/security/tls/ca.crt \
  -subj "/C=US/ST=CA/L=SF/O=Example/CN=CDC Pipeline CA"

# Server certificate
openssl genrsa -out configs/security/tls/server.key 2048
openssl req -new -key configs/security/tls/server.key -out configs/security/tls/server.csr \
  -subj "/C=US/ST=CA/L=SF/O=Example/CN=*.example.com"
openssl x509 -req -days 365 -in configs/security/tls/server.csr \
  -CA configs/security/tls/ca.crt -CAkey configs/security/tls/ca.key -CAcreateserial \
  -out configs/security/tls/server.crt

# Client certificate
openssl genrsa -out configs/security/tls/client.key 2048
openssl req -new -key configs/security/tls/client.key -out configs/security/tls/client.csr \
  -subj "/C=US/ST=CA/L=SF/O=Example/CN=kafka-connect"
openssl x509 -req -days 365 -in configs/security/tls/client.csr \
  -CA configs/security/tls/ca.crt -CAkey configs/security/tls/ca.key -CAcreateserial \
  -out configs/security/tls/client.crt
```

### Production Certificates

Use certificates from a trusted CA (Let's Encrypt, DigiCert, etc.):

```bash
# Let's Encrypt via certbot
certbot certonly --standalone -d kafka.example.com -d schema-registry.example.com

# Copy to configs/security/tls/
cp /etc/letsencrypt/live/kafka.example.com/fullchain.pem configs/security/tls/server.crt
cp /etc/letsencrypt/live/kafka.example.com/privkey.pem configs/security/tls/server.key
```

## Secrets Rotation

### Rotate Database Credentials

```bash
# 1. Create new credentials in Vault
vault kv put secret/postgres-credentials-new username=postgres_user password=<new-password>

# 2. Update connector configuration
vim configs/connectors/postgres-sink.json
# Change: ${file:/vault/secrets/postgres-credentials-new:password}

# 3. Restart connector
./scripts/deploy-connector.sh update postgres-sink

# 4. Revoke old credentials
vault kv delete secret/postgres-credentials
```

### Rotate TLS Certificates

```bash
# 1. Generate new certificates
./scripts/generate-certs.sh server postgres-new

# 2. Update service configuration
vim docker-compose.security.yml
# Update certificate paths

# 3. Rolling restart services
docker-compose -f docker-compose.yml -f docker-compose.security.yml up -d --no-deps postgres

# 4. Verify connectivity
./scripts/health-check.sh
```

## Vulnerability Management

### Dependency Scanning

```bash
# Scan Python dependencies
pip install safety
safety check --file requirements.txt

# Scan container images
docker scan kafka-connect:latest
docker scan postgres:14

# Scan infrastructure as code
checkov -d .
```

### Regular Updates

- **OS patches**: Monthly
- **Database updates**: Quarterly
- **Kafka updates**: Semi-annually
- **Python dependencies**: Monthly

## Incident Response

### Security Incident Procedure

1. **Detection**: Alert from monitoring or security tools
2. **Containment**: Isolate affected systems
3. **Investigation**: Analyze logs, identify root cause
4. **Remediation**: Patch vulnerabilities, update credentials
5. **Recovery**: Restore normal operations
6. **Post-mortem**: Document lessons learned

### Emergency Contacts

- **Security Team**: security@example.com
- **On-call Engineer**: [PagerDuty]
- **Compliance Officer**: compliance@example.com

## References

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CIS Benchmarks](https://www.cisecurity.org/cis-benchmarks/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [Kafka Security](https://kafka.apache.org/documentation/#security)
- [PostgreSQL Security](https://www.postgresql.org/docs/current/security.html)
- [ScyllaDB Security](https://docs.scylladb.com/stable/operating-scylla/security/)

---

**Last Updated**: 2025-12-09
**Maintained By**: Security Team
**Review Cycle**: Quarterly
