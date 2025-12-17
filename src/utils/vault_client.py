"""
Vault Client Utility for CDC Pipeline

Provides a secure interface to HashiCorp Vault for retrieving secrets
used by the CDC pipeline components.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
import hvac
from hvac.exceptions import VaultError, InvalidPath
import logging

logger = logging.getLogger(__name__)


@dataclass
class HealthStatus:
    """
    Structured health status for Vault client.

    Attributes:
        healthy: Overall health status (True if healthy)
        authenticated: Whether client is authenticated
        sealed: Whether Vault is sealed
        error: Error message if health check failed
    """

    healthy: bool
    authenticated: bool
    sealed: bool
    error: Optional[str] = None

    def __bool__(self) -> bool:
        """Allow boolean checks for backward compatibility."""
        return self.healthy


class VaultClient:
    """
    Client for interacting with HashiCorp Vault.

    Provides methods to securely retrieve database credentials and other secrets
    required by the CDC pipeline.
    """

    def __init__(
        self,
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        verify_ssl: bool = True,
        mount_point: str = "secret"
    ):
        """
        Initialize Vault client.

        Args:
            vault_url: Vault server URL (defaults to VAULT_ADDR env var)
            vault_token: Vault authentication token (defaults to VAULT_TOKEN env var)
            verify_ssl: Whether to verify SSL certificates
            mount_point: KV secrets engine mount point

        Raises:
            ValueError: If required parameters are missing
            VaultError: If connection to Vault fails
        """
        self.vault_url = vault_url or os.getenv("VAULT_ADDR")
        self.vault_token = vault_token or os.getenv("VAULT_TOKEN")
        self.mount_point = mount_point

        if not self.vault_url:
            raise ValueError("Vault URL must be provided via parameter or VAULT_ADDR environment variable")

        if not self.vault_token:
            raise ValueError("Vault token must be provided via parameter or VAULT_TOKEN environment variable")

        try:
            self.client = hvac.Client(
                url=self.vault_url,
                token=self.vault_token,
                verify=verify_ssl
            )

            if not self.client.is_authenticated():
                raise VaultError("Failed to authenticate with Vault")

            logger.info(f"Successfully connected to Vault at {self.vault_url}")

        except Exception as e:
            logger.error(f"Failed to initialize Vault client: {e}")
            raise VaultError(f"Vault initialization failed: {e}")

    def get_secret(self, path: str) -> Dict[str, Any]:
        """
        Retrieve a secret from Vault.

        Args:
            path: Secret path (e.g., "scylla-credentials")

        Returns:
            Dictionary containing secret data

        Raises:
            InvalidPath: If secret path does not exist
            VaultError: If retrieval fails
        """
        full_path = f"{self.mount_point}/data/{path}"

        try:
            logger.debug(f"Retrieving secret from path: {full_path}")
            response = self.client.secrets.kv.v2.read_secret_version(
                path=path,
                mount_point=self.mount_point
            )

            if not response or "data" not in response:
                raise InvalidPath(f"No data found at path: {path}")

            secret_data = response["data"].get("data", {})
            logger.info(f"Successfully retrieved secret from {path}")

            return secret_data

        except InvalidPath:
            logger.error(f"Secret not found at path: {path}")
            raise
        except Exception as e:
            logger.error(f"Failed to retrieve secret from {path}: {e}")
            raise VaultError(f"Secret retrieval failed: {e}")

    def get_database_credentials(self, database: str) -> Dict[str, str]:
        """
        Retrieve database credentials from Vault.

        Args:
            database: Database identifier ("scylla" or "postgres")

        Returns:
            Dictionary with database credentials (username, password, host, etc.)

        Raises:
            ValueError: If database identifier is invalid
            VaultError: If retrieval fails
        """
        valid_databases = ["scylla", "postgres"]

        if database not in valid_databases:
            raise ValueError(f"Invalid database: {database}. Must be one of {valid_databases}")

        path = f"{database}-credentials"

        try:
            credentials = self.get_secret(path)
            logger.info(f"Retrieved credentials for {database}")
            return credentials

        except Exception as e:
            logger.error(f"Failed to get {database} credentials: {e}")
            raise

    def get_scylla_credentials(self) -> Dict[str, str]:
        """
        Retrieve ScyllaDB credentials from Vault.

        Returns:
            Dictionary with ScyllaDB connection parameters
        """
        return self.get_database_credentials("scylla")

    def get_postgres_credentials(self) -> Dict[str, str]:
        """
        Retrieve PostgreSQL credentials from Vault.

        Returns:
            Dictionary with PostgreSQL connection parameters
        """
        return self.get_database_credentials("postgres")

    def get_kafka_credentials(self) -> Dict[str, str]:
        """
        Retrieve Kafka credentials from Vault.

        Returns:
            Dictionary with Kafka connection parameters
        """
        try:
            credentials = self.get_secret("kafka-credentials")
            logger.info("Retrieved Kafka credentials")
            return credentials
        except Exception as e:
            logger.error(f"Failed to get Kafka credentials: {e}")
            raise

    def health_check(self) -> HealthStatus:
        """
        Check if Vault is accessible and authenticated.

        Returns:
            HealthStatus object with detailed health information.
            Can be used as boolean for backward compatibility.

        Examples:
            >>> vault = VaultClient()
            >>> status = vault.health_check()
            >>> if status:  # Boolean check (backward compatible)
            ...     print("Vault is healthy")
            >>> print(f"Authenticated: {status.authenticated}")
            >>> print(f"Sealed: {status.sealed}")
            >>> if status.error:
            ...     print(f"Error: {status.error}")
        """
        try:
            # Check authentication
            is_authenticated = self.client.is_authenticated()

            if not is_authenticated:
                logger.warning("Vault authentication check failed")
                return HealthStatus(
                    healthy=False,
                    authenticated=False,
                    sealed=True,
                    error="Not authenticated"
                )

            # Check seal status
            health = self.client.sys.read_health_status()
            is_sealed = health.get("sealed", True)
            is_healthy = is_authenticated and not is_sealed

            if is_healthy:
                logger.info("Vault health check passed")
            else:
                logger.warning("Vault is sealed")

            return HealthStatus(
                healthy=is_healthy,
                authenticated=is_authenticated,
                sealed=is_sealed,
                error=None if is_healthy else "Vault is sealed"
            )

        except Exception as e:
            logger.error(f"Vault health check failed: {e}")
            return HealthStatus(
                healthy=False,
                authenticated=False,
                sealed=True,
                error=str(e)
            )

    def list_secrets(self, path: str = "") -> list:
        """
        List secrets at a given path.

        Args:
            path: Path to list secrets from (relative to mount point)

        Returns:
            List of secret names
        """
        try:
            response = self.client.secrets.kv.v2.list_secrets(
                path=path,
                mount_point=self.mount_point
            )

            keys = response.get("data", {}).get("keys", [])
            logger.info(f"Listed {len(keys)} secrets at path: {path}")

            return keys

        except Exception as e:
            logger.error(f"Failed to list secrets at {path}: {e}")
            raise VaultError(f"Secret listing failed: {e}")

    def close(self):
        """Close the Vault client connection."""
        self.client = None
        logger.info("Vault client connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
