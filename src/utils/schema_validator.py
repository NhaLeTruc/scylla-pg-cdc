"""
Schema Validator Utility for CDC Pipeline

Validates Avro schemas and ensures compatibility for schema evolution
in the CDC pipeline.
"""

import json
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import logging
import requests

logger = logging.getLogger(__name__)


class CompatibilityMode(Enum):
    """Schema compatibility modes."""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


class SchemaType(Enum):
    """Supported schema types."""
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


class SchemaCompatibilityError(Exception):
    """Raised when schema compatibility check fails."""
    pass


class SchemaRegistryError(Exception):
    """Raised when Schema Registry operations fail."""
    pass


class SchemaValidator:
    """
    Validator for CDC pipeline schemas.

    Provides methods to validate schema structure and check compatibility
    between schema versions.
    """

    def __init__(self, compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD, schema_registry_url: Optional[str] = None):
        """
        Initialize schema validator.

        Args:
            compatibility_mode: Schema compatibility mode to enforce
            schema_registry_url: URL of Schema Registry (e.g., http://localhost:8081)
        """
        self.compatibility_mode = compatibility_mode
        self.schema_registry_url = schema_registry_url
        self.schema_registry_client = None

        if schema_registry_url:
            self._init_schema_registry_client()

        logger.info(f"Initialized SchemaValidator with {compatibility_mode.value} compatibility")

    def _init_schema_registry_client(self):
        """Initialize Schema Registry HTTP client."""
        class SimpleSchemaRegistryClient:
            """Simple HTTP client for Schema Registry."""

            def __init__(self, url: str):
                self.url = url.rstrip('/')
                self.session = requests.Session()
                self.session.headers.update({"Content-Type": "application/vnd.schemaregistry.v1+json"})

            def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
                """Get schema by ID."""
                response = self.session.get(f"{self.url}/schemas/ids/{schema_id}")
                response.raise_for_status()
                return response.json()["schema"]

            def get_latest_schema_version(self, subject: str) -> Tuple[int, Dict[str, Any]]:
                """Get latest schema version for a subject."""
                response = self.session.get(f"{self.url}/subjects/{subject}/versions/latest")
                response.raise_for_status()
                data = response.json()
                return data["version"], json.loads(data["schema"])

            def get_all_versions(self, subject: str) -> List[Tuple[int, Dict[str, Any]]]:
                """Get all versions for a subject."""
                response = self.session.get(f"{self.url}/subjects/{subject}/versions")
                response.raise_for_status()
                versions = response.json()

                results = []
                for version in versions:
                    resp = self.session.get(f"{self.url}/subjects/{subject}/versions/{version}")
                    resp.raise_for_status()
                    data = resp.json()
                    results.append((data["version"], json.loads(data["schema"])))

                return results

            def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
                """Register a new schema."""
                payload = {"schema": json.dumps(schema)}
                response = self.session.post(f"{self.url}/subjects/{subject}/versions", json=payload)
                response.raise_for_status()
                return response.json()["id"]

            def test_compatibility(self, subject: str, schema: Dict[str, Any]) -> bool:
                """Test if schema is compatible."""
                payload = {"schema": json.dumps(schema)}
                response = self.session.post(f"{self.url}/compatibility/subjects/{subject}/versions/latest", json=payload)
                response.raise_for_status()
                return response.json()["is_compatible"]

            def get_schema_by_version(self, subject: str, version: int) -> Dict[str, Any]:
                """Get schema by subject and version."""
                response = self.session.get(f"{self.url}/subjects/{subject}/versions/{version}")
                response.raise_for_status()
                return json.loads(response.json()["schema"])

            def list_subjects(self) -> List[str]:
                """List all subjects."""
                response = self.session.get(f"{self.url}/subjects")
                response.raise_for_status()
                return response.json()

            def delete_schema_version(self, subject: str, version: int) -> bool:
                """Delete a schema version."""
                response = self.session.delete(f"{self.url}/subjects/{subject}/versions/{version}")
                response.raise_for_status()
                return True

            def get_compatibility(self, subject: str) -> str:
                """Get compatibility mode for subject."""
                response = self.session.get(f"{self.url}/config/{subject}")
                response.raise_for_status()
                return response.json()["compatibilityLevel"]

            def set_compatibility(self, subject: str, level: str) -> bool:
                """Set compatibility mode for subject."""
                payload = {"compatibility": level}
                response = self.session.put(f"{self.url}/config/{subject}", json=payload)
                response.raise_for_status()
                return True

        self.schema_registry_client = SimpleSchemaRegistryClient(self.schema_registry_url)
        logger.info(f"Initialized Schema Registry client for {self.schema_registry_url}")

    def validate_avro_schema(self, schema: Dict[str, Any]) -> bool:
        """
        Validate an Avro schema structure.

        Args:
            schema: Avro schema dictionary

        Returns:
            True if schema is valid

        Raises:
            SchemaValidationError: If schema is invalid
        """
        if not isinstance(schema, dict):
            raise SchemaValidationError("Schema must be a dictionary")

        # Check required fields
        required_fields = ["type", "name"]
        missing_fields = [field for field in required_fields if field not in schema]

        if missing_fields:
            raise SchemaValidationError(f"Schema missing required fields: {missing_fields}")

        # Validate type
        valid_types = ["record", "enum", "array", "map", "fixed"]
        schema_type = schema.get("type")

        if schema_type not in valid_types and not isinstance(schema_type, dict):
            raise SchemaValidationError(f"Invalid schema type: {schema_type}")

        # Validate fields if it's a record type
        if schema_type == "record":
            if "fields" not in schema:
                raise SchemaValidationError("Record schema must have 'fields' property")

            if not isinstance(schema["fields"], list):
                raise SchemaValidationError("Schema 'fields' must be a list")

            for field in schema["fields"]:
                self._validate_field(field)

        logger.debug(f"Schema validation passed for: {schema.get('name')}")
        return True

    def _validate_field(self, field: Dict[str, Any]) -> None:
        """
        Validate a single Avro field.

        Args:
            field: Field dictionary

        Raises:
            SchemaValidationError: If field is invalid
        """
        if not isinstance(field, dict):
            raise SchemaValidationError("Field must be a dictionary")

        if "name" not in field:
            raise SchemaValidationError("Field must have a 'name' property")

        if "type" not in field:
            raise SchemaValidationError(f"Field '{field['name']}' must have a 'type' property")

    def check_backward_compatibility(
        self,
        new_schema: Dict[str, Any],
        old_schema: Dict[str, Any]
    ) -> bool:
        """
        Check if new schema is backward compatible with old schema.

        Backward compatibility means new schema can read data written with old schema.

        Args:
            new_schema: New schema version
            old_schema: Previous schema version

        Returns:
            True if schemas are compatible

        Raises:
            SchemaCompatibilityError: If schemas are incompatible
        """
        logger.debug("Checking backward compatibility")

        # Validate both schemas first
        self.validate_avro_schema(new_schema)
        self.validate_avro_schema(old_schema)

        # Check if it's the same schema type
        if new_schema.get("type") != old_schema.get("type"):
            raise SchemaCompatibilityError(
                f"Schema type changed from {old_schema.get('type')} to {new_schema.get('type')}"
            )

        # For record types, check field compatibility
        if new_schema.get("type") == "record":
            old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
            new_fields = {f["name"]: f for f in new_schema.get("fields", [])}

            # All old fields must exist in new schema or have defaults
            for field_name, old_field in old_fields.items():
                if field_name not in new_fields:
                    raise SchemaCompatibilityError(
                        f"Field '{field_name}' removed without default value"
                    )

                new_field = new_fields[field_name]

                # Check if field type changed
                if not self._is_type_compatible(new_field["type"], old_field["type"]):
                    raise SchemaCompatibilityError(
                        f"Field '{field_name}' type changed incompatibly"
                    )

            # New fields must have defaults for backward compatibility
            for field_name, new_field in new_fields.items():
                if field_name not in old_fields and "default" not in new_field:
                    raise SchemaCompatibilityError(
                        f"New field '{field_name}' added without default value"
                    )

        logger.info("Backward compatibility check passed")
        return True

    def check_forward_compatibility(
        self,
        new_schema: Dict[str, Any],
        old_schema: Dict[str, Any]
    ) -> bool:
        """
        Check if new schema is forward compatible with old schema.

        Forward compatibility means old schema can read data written with new schema.

        Args:
            new_schema: New schema version
            old_schema: Previous schema version

        Returns:
            True if schemas are compatible

        Raises:
            SchemaCompatibilityError: If schemas are incompatible
        """
        logger.debug("Checking forward compatibility")

        # Forward compatibility is the reverse of backward compatibility
        return self.check_backward_compatibility(old_schema, new_schema)

    def check_full_compatibility(
        self,
        new_schema: Dict[str, Any],
        old_schema: Dict[str, Any]
    ) -> bool:
        """
        Check if schemas are both backward and forward compatible.

        Args:
            new_schema: New schema version
            old_schema: Previous schema version

        Returns:
            True if schemas are fully compatible

        Raises:
            SchemaCompatibilityError: If schemas are incompatible
        """
        logger.debug("Checking full compatibility")

        self.check_backward_compatibility(new_schema, old_schema)
        self.check_forward_compatibility(new_schema, old_schema)

        logger.info("Full compatibility check passed")
        return True

    def check_compatibility(
        self,
        new_schema: Dict[str, Any],
        old_schema: Dict[str, Any],
        mode: Optional[CompatibilityMode] = None
    ) -> bool:
        """
        Check schema compatibility based on mode.

        Args:
            new_schema: New schema version
            old_schema: Previous schema version
            mode: Compatibility mode (uses instance default if not provided)

        Returns:
            True if schemas are compatible

        Raises:
            SchemaCompatibilityError: If schemas are incompatible
        """
        mode = mode or self.compatibility_mode

        if mode == CompatibilityMode.BACKWARD:
            return self.check_backward_compatibility(new_schema, old_schema)
        elif mode == CompatibilityMode.FORWARD:
            return self.check_forward_compatibility(new_schema, old_schema)
        elif mode == CompatibilityMode.FULL:
            return self.check_full_compatibility(new_schema, old_schema)
        elif mode == CompatibilityMode.NONE:
            logger.info("Compatibility checking disabled")
            return True
        else:
            raise ValueError(f"Unknown compatibility mode: {mode}")

    def _is_type_compatible(self, new_type: Any, old_type: Any) -> bool:
        """
        Check if two field types are compatible.

        Args:
            new_type: New field type
            old_type: Old field type

        Returns:
            True if types are compatible
        """
        # Exact match
        if new_type == old_type:
            return True

        # Handle union types
        if isinstance(new_type, list) and isinstance(old_type, list):
            # New union must contain all old types
            return all(ot in new_type for ot in old_type)

        # Handle nullable types (union with null)
        if isinstance(new_type, list) and "null" in new_type:
            non_null_types = [t for t in new_type if t != "null"]
            if len(non_null_types) == 1:
                return self._is_type_compatible(non_null_types[0], old_type)

        if isinstance(old_type, list) and "null" in old_type:
            non_null_types = [t for t in old_type if t != "null"]
            if len(non_null_types) == 1:
                return self._is_type_compatible(new_type, non_null_types[0])

        # Type promotions (e.g., int -> long)
        type_promotions = {
            "int": ["long", "float", "double"],
            "long": ["float", "double"],
            "float": ["double"],
            "string": ["bytes"]
        }

        if old_type in type_promotions:
            return new_type in type_promotions[old_type]

        return False

    def parse_schema(self, schema_str: str) -> Dict[str, Any]:
        """
        Parse schema from JSON string.

        Args:
            schema_str: JSON schema string

        Returns:
            Parsed schema dictionary

        Raises:
            SchemaValidationError: If parsing fails
        """
        try:
            schema = json.loads(schema_str)
            return schema
        except json.JSONDecodeError as e:
            raise SchemaValidationError(f"Failed to parse schema: {e}")

    def get_schema_fingerprint(self, schema: Dict[str, Any]) -> str:
        """
        Generate a fingerprint for the schema.

        Args:
            schema: Schema dictionary

        Returns:
            Schema fingerprint (canonical JSON string)
        """
        import hashlib

        canonical = json.dumps(schema, sort_keys=True, separators=(',', ':'))
        fingerprint = hashlib.sha256(canonical.encode()).hexdigest()

        logger.debug(f"Generated schema fingerprint: {fingerprint}")
        return fingerprint

    # ============================================================================
    # Schema Registry Integration Methods
    # ============================================================================

    def _ensure_registry_client(self):
        """Ensure Schema Registry client is configured."""
        if not self.schema_registry_client:
            raise SchemaRegistryError("Schema Registry client not configured")

    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """
        Get schema from registry by ID.

        Args:
            schema_id: Schema ID in registry

        Returns:
            Schema dictionary

        Raises:
            SchemaRegistryError: If schema not found or registry error
        """
        self._ensure_registry_client()

        try:
            schema = self.schema_registry_client.get_schema_by_id(schema_id)
            logger.info(f"Retrieved schema {schema_id} from registry")
            return schema
        except Exception as e:
            raise SchemaRegistryError(f"Failed to get schema {schema_id}: {e}")

    def get_latest_schema_version(self, subject: str) -> Tuple[int, Dict[str, Any]]:
        """
        Get latest schema version for a subject.

        Args:
            subject: Subject name (e.g., "topic-value")

        Returns:
            Tuple of (version number, schema dictionary)

        Raises:
            SchemaRegistryError: If subject not found or registry error
        """
        self._ensure_registry_client()

        try:
            version, schema = self.schema_registry_client.get_latest_schema_version(subject)
            logger.info(f"Retrieved latest schema for {subject}: version {version}")
            return version, schema
        except Exception as e:
            raise SchemaRegistryError(f"Failed to get latest schema for {subject}: {e}")

    def get_all_schema_versions(self, subject: str) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Get all schema versions for a subject.

        Args:
            subject: Subject name

        Returns:
            List of (version, schema) tuples

        Raises:
            SchemaRegistryError: If subject not found or registry error
        """
        self._ensure_registry_client()

        try:
            versions = self.schema_registry_client.get_all_versions(subject)
            logger.info(f"Retrieved {len(versions)} version(s) for {subject}")
            return versions
        except Exception as e:
            raise SchemaRegistryError(f"Failed to get versions for {subject}: {e}")

    def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """
        Register a new schema with the registry.

        Args:
            subject: Subject name
            schema: Schema to register

        Returns:
            Schema ID

        Raises:
            SchemaRegistryError: If registration fails
        """
        self._ensure_registry_client()

        try:
            schema_id = self.schema_registry_client.register_schema(subject, schema)
            logger.info(f"Registered schema for {subject}: ID {schema_id}")
            return schema_id
        except Exception as e:
            raise SchemaRegistryError(f"Failed to register schema for {subject}: {e}")

    def test_compatibility_with_registry(self, subject: str, schema: Dict[str, Any]) -> bool:
        """
        Test if schema is compatible with registered schemas.

        Args:
            subject: Subject name
            schema: Schema to test

        Returns:
            True if compatible, False otherwise

        Raises:
            SchemaRegistryError: If registry error
        """
        self._ensure_registry_client()

        try:
            is_compatible = self.schema_registry_client.test_compatibility(subject, schema)
            logger.info(f"Compatibility test for {subject}: {is_compatible}")
            return is_compatible
        except Exception as e:
            raise SchemaRegistryError(f"Failed to test compatibility for {subject}: {e}")

    def get_schema_diff(self, subject: str, version1: int, version2: int) -> Dict[str, List[str]]:
        """
        Get diff between two schema versions.

        Args:
            subject: Subject name
            version1: First version number
            version2: Second version number

        Returns:
            Dictionary with added_fields, removed_fields, and changed_fields

        Raises:
            SchemaRegistryError: If versions not found
        """
        self._ensure_registry_client()

        try:
            schema1 = self.schema_registry_client.get_schema_by_version(subject, version1)
            schema2 = self.schema_registry_client.get_schema_by_version(subject, version2)

            # Calculate diff for record types
            diff = {
                "added_fields": [],
                "removed_fields": [],
                "changed_fields": []
            }

            if schema1.get("type") == "record" and schema2.get("type") == "record":
                fields1 = {f["name"]: f for f in schema1.get("fields", [])}
                fields2 = {f["name"]: f for f in schema2.get("fields", [])}

                # Added fields
                diff["added_fields"] = [name for name in fields2 if name not in fields1]

                # Removed fields
                diff["removed_fields"] = [name for name in fields1 if name not in fields2]

                # Changed fields
                for name in fields1:
                    if name in fields2 and fields1[name]["type"] != fields2[name]["type"]:
                        diff["changed_fields"].append(name)

            logger.info(f"Schema diff for {subject} v{version1}->v{version2}: "
                        f"{len(diff['added_fields'])} added, {len(diff['removed_fields'])} removed, "
                        f"{len(diff['changed_fields'])} changed")

            return diff
        except Exception as e:
            raise SchemaRegistryError(f"Failed to get schema diff: {e}")

    def list_subjects(self) -> List[str]:
        """
        List all subjects in the registry.

        Returns:
            List of subject names

        Raises:
            SchemaRegistryError: If registry error
        """
        self._ensure_registry_client()

        try:
            subjects = self.schema_registry_client.list_subjects()
            logger.info(f"Found {len(subjects)} subject(s) in registry")
            return subjects
        except Exception as e:
            raise SchemaRegistryError(f"Failed to list subjects: {e}")

    def delete_schema_version(self, subject: str, version: int) -> bool:
        """
        Delete a specific schema version.

        Args:
            subject: Subject name
            version: Version to delete

        Returns:
            True if deleted successfully

        Raises:
            SchemaRegistryError: If deletion fails
        """
        self._ensure_registry_client()

        try:
            result = self.schema_registry_client.delete_schema_version(subject, version)
            logger.info(f"Deleted schema version {version} for {subject}")
            return result
        except Exception as e:
            raise SchemaRegistryError(f"Failed to delete schema version: {e}")

    def get_registry_compatibility_mode(self, subject: str) -> CompatibilityMode:
        """
        Get compatibility mode from registry for a subject.

        Args:
            subject: Subject name

        Returns:
            CompatibilityMode

        Raises:
            SchemaRegistryError: If registry error
        """
        self._ensure_registry_client()

        try:
            mode_str = self.schema_registry_client.get_compatibility(subject)
            mode = CompatibilityMode[mode_str]
            logger.info(f"Compatibility mode for {subject}: {mode.value}")
            return mode
        except Exception as e:
            raise SchemaRegistryError(f"Failed to get compatibility mode: {e}")

    def set_registry_compatibility_mode(self, subject: str, mode: CompatibilityMode) -> bool:
        """
        Set compatibility mode in registry for a subject.

        Args:
            subject: Subject name
            mode: CompatibilityMode to set

        Returns:
            True if set successfully

        Raises:
            SchemaRegistryError: If registry error
        """
        self._ensure_registry_client()

        try:
            result = self.schema_registry_client.set_compatibility(subject, mode.value)
            logger.info(f"Set compatibility mode for {subject} to {mode.value}")
            return result
        except Exception as e:
            raise SchemaRegistryError(f"Failed to set compatibility mode: {e}")
