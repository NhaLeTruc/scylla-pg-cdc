"""
Schema Validator Utility for CDC Pipeline

Validates Avro schemas and ensures compatibility for schema evolution
in the CDC pipeline.
"""

import json
from typing import Dict, List, Optional, Any
from enum import Enum
import logging

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


class SchemaValidator:
    """
    Validator for CDC pipeline schemas.

    Provides methods to validate schema structure and check compatibility
    between schema versions.
    """

    def __init__(self, compatibility_mode: CompatibilityMode = CompatibilityMode.BACKWARD):
        """
        Initialize schema validator.

        Args:
            compatibility_mode: Schema compatibility mode to enforce
        """
        self.compatibility_mode = compatibility_mode
        logger.info(f"Initialized SchemaValidator with {compatibility_mode.value} compatibility")

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
