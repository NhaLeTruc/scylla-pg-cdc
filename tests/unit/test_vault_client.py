"""
Unit tests for vault_client module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from hvac.exceptions import VaultError, InvalidPath

from src.utils.vault_client import VaultClient


class TestVaultClient:
    """Test suite for VaultClient class."""

    @pytest.fixture
    def mock_hvac_client(self):
        """Mock hvac.Client for testing."""
        with patch('src.utils.vault_client.hvac.Client') as mock:
            client_instance = MagicMock()
            client_instance.is_authenticated.return_value = True
            mock.return_value = client_instance
            yield mock

    def test_init_with_parameters(self, mock_hvac_client):
        """Test VaultClient initialization with explicit parameters."""
        client = VaultClient(
            vault_url="http://test-vault:8200",
            vault_token="test-token"
        )

        assert client.vault_url == "http://test-vault:8200"
        assert client.vault_token == "test-token"
        assert client.mount_point == "secret"
        mock_hvac_client.assert_called_once()

    def test_init_with_env_vars(self, mock_hvac_client, monkeypatch):
        """Test VaultClient initialization with environment variables."""
        monkeypatch.setenv("VAULT_ADDR", "http://env-vault:8200")
        monkeypatch.setenv("VAULT_TOKEN", "env-token")

        client = VaultClient()

        assert client.vault_url == "http://env-vault:8200"
        assert client.vault_token == "env-token"

    def test_init_missing_url_raises_error(self, monkeypatch):
        """Test that missing Vault URL raises ValueError."""
        monkeypatch.delenv("VAULT_ADDR", raising=False)
        with pytest.raises(ValueError, match="Vault URL must be provided"):
            VaultClient(vault_token="test-token")

    def test_init_missing_token_raises_error(self, monkeypatch):
        """Test that missing Vault token raises ValueError."""
        monkeypatch.delenv("VAULT_TOKEN", raising=False)
        with pytest.raises(ValueError, match="Vault token must be provided"):
            VaultClient(vault_url="http://test:8200")

    def test_init_authentication_failure(self, mock_hvac_client):
        """Test that authentication failure raises VaultError."""
        mock_hvac_client.return_value.is_authenticated.return_value = False

        with pytest.raises(VaultError, match="Failed to authenticate"):
            VaultClient(vault_url="http://test:8200", vault_token="bad-token")

    def test_get_secret_success(self, mock_hvac_client):
        """Test successful secret retrieval."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "username": "testuser",
                    "password": "testpass"
                }
            }
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        secret = client.get_secret("test-credentials")

        assert secret == {"username": "testuser", "password": "testpass"}
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="test-credentials",
            mount_point="secret"
        )

    def test_get_secret_not_found(self, mock_hvac_client):
        """Test secret retrieval with invalid path."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.side_effect = InvalidPath("Not found")

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")

        with pytest.raises(InvalidPath):
            client.get_secret("nonexistent")

    def test_get_secret_empty_response(self, mock_hvac_client):
        """Test secret retrieval with empty response."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {}

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")

        with pytest.raises(InvalidPath, match="No data found"):
            client.get_secret("empty-secret")

    def test_get_database_credentials_scylla(self, mock_hvac_client):
        """Test retrieving ScyllaDB credentials."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "username": "scylla-user",
                    "password": "scylla-pass"
                }
            }
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        creds = client.get_database_credentials("scylla")

        assert creds["username"] == "scylla-user"
        assert creds["password"] == "scylla-pass"

    def test_get_database_credentials_postgres(self, mock_hvac_client):
        """Test retrieving PostgreSQL credentials."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {
                "data": {
                    "username": "postgres-user",
                    "password": "postgres-pass"
                }
            }
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        creds = client.get_database_credentials("postgres")

        assert creds["username"] == "postgres-user"
        assert creds["password"] == "postgres-pass"

    def test_get_database_credentials_invalid_database(self, mock_hvac_client):
        """Test that invalid database name raises ValueError."""
        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")

        with pytest.raises(ValueError, match="Invalid database"):
            client.get_database_credentials("invalid")

    def test_get_scylla_credentials(self, mock_hvac_client):
        """Test get_scylla_credentials convenience method."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"username": "scylla"}}
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        creds = client.get_scylla_credentials()

        assert creds["username"] == "scylla"

    def test_get_postgres_credentials(self, mock_hvac_client):
        """Test get_postgres_credentials convenience method."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"username": "postgres"}}
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        creds = client.get_postgres_credentials()

        assert creds["username"] == "postgres"

    def test_get_kafka_credentials(self, mock_hvac_client):
        """Test get_kafka_credentials method."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"bootstrap_servers": "kafka:9092"}}
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        creds = client.get_kafka_credentials()

        assert creds["bootstrap_servers"] == "kafka:9092"

    def test_health_check_success(self, mock_hvac_client):
        """Test successful health check."""
        mock_client = mock_hvac_client.return_value
        mock_client.is_authenticated.return_value = True
        mock_client.sys.read_health_status.return_value = {"sealed": False}

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        assert client.health_check() is True

    def test_health_check_not_authenticated(self, mock_hvac_client):
        """Test health check with failed authentication."""
        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")

        # After client is created, change authentication to fail
        mock_client = mock_hvac_client.return_value
        mock_client.is_authenticated.return_value = False

        assert client.health_check() is False

    def test_health_check_sealed(self, mock_hvac_client):
        """Test health check with sealed Vault."""
        mock_client = mock_hvac_client.return_value
        mock_client.is_authenticated.return_value = True
        mock_client.sys.read_health_status.return_value = {"sealed": True}

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        client.client = mock_client
        assert client.health_check() is False

    def test_list_secrets_success(self, mock_hvac_client):
        """Test listing secrets."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.list_secrets.return_value = {
            "data": {"keys": ["secret1", "secret2", "secret3"]}
        }

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        secrets = client.list_secrets()

        assert secrets == ["secret1", "secret2", "secret3"]

    def test_list_secrets_empty(self, mock_hvac_client):
        """Test listing secrets with empty result."""
        mock_client = mock_hvac_client.return_value
        mock_client.secrets.kv.v2.list_secrets.return_value = {"data": {}}

        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        secrets = client.list_secrets()

        assert secrets == []

    def test_context_manager(self, mock_hvac_client):
        """Test VaultClient as context manager."""
        with VaultClient(vault_url="http://test:8200", vault_token="test-token") as client:
            assert client is not None
            assert client.client is not None

        assert client.client is None

    def test_close(self, mock_hvac_client):
        """Test closing the Vault client."""
        client = VaultClient(vault_url="http://test:8200", vault_token="test-token")
        assert client.client is not None

        client.close()
        assert client.client is None
