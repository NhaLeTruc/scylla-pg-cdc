"""
Unit tests for schema_validator module.
"""

import pytest
import json
from src.utils.schema_validator import (
    SchemaValidator,
    SchemaValidationError,
    SchemaCompatibilityError,
    CompatibilityMode,
    SchemaType
)


class TestSchemaValidation:
    """Test schema validation functionality."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator instance."""
        return SchemaValidator()

    def test_validate_avro_schema_valid_record(self, validator):
        """Test validation of valid Avro record schema."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }

        assert validator.validate_avro_schema(schema) is True

    def test_validate_avro_schema_missing_type(self, validator):
        """Test that schema without type raises error."""
        schema = {"name": "User"}

        with pytest.raises(SchemaValidationError, match="missing required fields"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_missing_name(self, validator):
        """Test that schema without name raises error."""
        schema = {"type": "record"}

        with pytest.raises(SchemaValidationError, match="missing required fields"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_not_dict(self, validator):
        """Test that non-dictionary schema raises error."""
        with pytest.raises(SchemaValidationError, match="must be a dictionary"):
            validator.validate_avro_schema("not a dict")

    def test_validate_avro_schema_invalid_type(self, validator):
        """Test that invalid type raises error."""
        schema = {"type": "invalid_type", "name": "Test"}

        with pytest.raises(SchemaValidationError, match="Invalid schema type"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_record_without_fields(self, validator):
        """Test that record without fields raises error."""
        schema = {"type": "record", "name": "User"}

        with pytest.raises(SchemaValidationError, match="must have 'fields'"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_fields_not_list(self, validator):
        """Test that non-list fields raises error."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": "not a list"
        }

        with pytest.raises(SchemaValidationError, match="'fields' must be a list"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_field_without_name(self, validator):
        """Test that field without name raises error."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"type": "string"}]
        }

        with pytest.raises(SchemaValidationError, match="must have a 'name'"):
            validator.validate_avro_schema(schema)

    def test_validate_avro_schema_field_without_type(self, validator):
        """Test that field without type raises error."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id"}]
        }

        with pytest.raises(SchemaValidationError, match="must have a 'type'"):
            validator.validate_avro_schema(schema)


class TestBackwardCompatibility:
    """Test backward compatibility checking."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator with backward compatibility mode."""
        return SchemaValidator(CompatibilityMode.BACKWARD)

    def test_backward_compatible_add_field_with_default(self, validator):
        """Test adding field with default is backward compatible."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        }

        assert validator.check_backward_compatibility(new_schema, old_schema) is True

    def test_backward_incompatible_add_field_without_default(self, validator):
        """Test that adding field without default is incompatible."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }

        with pytest.raises(SchemaCompatibilityError, match="without default"):
            validator.check_backward_compatibility(new_schema, old_schema)

    def test_backward_incompatible_remove_field(self, validator):
        """Test that removing field is incompatible."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        with pytest.raises(SchemaCompatibilityError, match="removed without default"):
            validator.check_backward_compatibility(new_schema, old_schema)

    def test_backward_incompatible_type_change(self, validator):
        """Test that changing field type is incompatible."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"}
            ]
        }

        with pytest.raises(SchemaCompatibilityError, match="type changed incompatibly"):
            validator.check_backward_compatibility(new_schema, old_schema)

    def test_backward_incompatible_schema_type_change(self, validator):
        """Test that changing schema type is incompatible."""
        old_schema = {"type": "record", "name": "User", "fields": []}
        new_schema = {"type": "enum", "name": "User", "symbols": []}

        with pytest.raises(SchemaCompatibilityError, match="Schema type changed"):
            validator.check_backward_compatibility(new_schema, old_schema)

    def test_backward_compatible_same_schema(self, validator):
        """Test that identical schemas are compatible."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        assert validator.check_backward_compatibility(schema, schema) is True


class TestForwardCompatibility:
    """Test forward compatibility checking."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator with forward compatibility mode."""
        return SchemaValidator(CompatibilityMode.FORWARD)

    def test_forward_compatible_remove_field(self, validator):
        """Test that removing field can be forward compatible."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        # Forward compatibility check is reverse of backward
        with pytest.raises(SchemaCompatibilityError):
            validator.check_forward_compatibility(new_schema, old_schema)


class TestFullCompatibility:
    """Test full compatibility checking."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator with full compatibility mode."""
        return SchemaValidator(CompatibilityMode.FULL)

    def test_full_compatible_schemas(self, validator):
        """Test fully compatible schemas."""
        schema1 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        schema2 = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        assert validator.check_full_compatibility(schema2, schema1) is True

    def test_full_incompatible_add_field_without_default(self, validator):
        """Test that adding field without default fails full compatibility."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"}
            ]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }

        with pytest.raises(SchemaCompatibilityError):
            validator.check_full_compatibility(new_schema, old_schema)


class TestTypeCompatibility:
    """Test type compatibility checking."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator instance."""
        return SchemaValidator()

    def test_exact_type_match_compatible(self, validator):
        """Test that exact type match is compatible."""
        assert validator._is_type_compatible("string", "string") is True
        assert validator._is_type_compatible("int", "int") is True

    def test_int_to_long_promotion_compatible(self, validator):
        """Test that int to long promotion is compatible."""
        assert validator._is_type_compatible("long", "int") is True
        assert validator._is_type_compatible("float", "int") is True
        assert validator._is_type_compatible("double", "int") is True

    def test_long_to_float_promotion_compatible(self, validator):
        """Test that long to float promotion is compatible."""
        assert validator._is_type_compatible("float", "long") is True
        assert validator._is_type_compatible("double", "long") is True

    def test_incompatible_type_promotion(self, validator):
        """Test that incompatible types are detected."""
        assert validator._is_type_compatible("int", "string") is False
        assert validator._is_type_compatible("string", "int") is False

    def test_union_type_compatibility(self, validator):
        """Test union type compatibility."""
        assert validator._is_type_compatible(["string", "null"], ["string", "null"]) is True
        assert validator._is_type_compatible(["string", "int", "null"], ["string", "null"]) is True


class TestSchemaUtilities:
    """Test schema utility functions."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator instance."""
        return SchemaValidator()

    def test_parse_schema_valid_json(self, validator):
        """Test parsing valid JSON schema."""
        schema_str = '{"type": "record", "name": "User", "fields": []}'
        schema = validator.parse_schema(schema_str)

        assert schema["type"] == "record"
        assert schema["name"] == "User"

    def test_parse_schema_invalid_json(self, validator):
        """Test that invalid JSON raises error."""
        with pytest.raises(SchemaValidationError, match="Failed to parse"):
            validator.parse_schema("not valid json")

    def test_get_schema_fingerprint(self, validator):
        """Test schema fingerprint generation."""
        schema = {"type": "record", "name": "User", "fields": []}
        fingerprint = validator.get_schema_fingerprint(schema)

        assert isinstance(fingerprint, str)
        assert len(fingerprint) == 64  # SHA256 hex digest length

    def test_get_schema_fingerprint_consistent(self, validator):
        """Test that fingerprint is consistent for same schema."""
        schema = {"type": "record", "name": "User", "fields": []}

        fingerprint1 = validator.get_schema_fingerprint(schema)
        fingerprint2 = validator.get_schema_fingerprint(schema)

        assert fingerprint1 == fingerprint2

    def test_get_schema_fingerprint_different_for_different_schemas(self, validator):
        """Test that different schemas have different fingerprints."""
        schema1 = {"type": "record", "name": "User", "fields": []}
        schema2 = {"type": "record", "name": "Product", "fields": []}

        fingerprint1 = validator.get_schema_fingerprint(schema1)
        fingerprint2 = validator.get_schema_fingerprint(schema2)

        assert fingerprint1 != fingerprint2


class TestCompatibilityModes:
    """Test compatibility mode functionality."""

    def test_check_compatibility_backward_mode(self):
        """Test check_compatibility with BACKWARD mode."""
        validator = SchemaValidator(CompatibilityMode.BACKWARD)

        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        }

        assert validator.check_compatibility(new_schema, old_schema) is True

    def test_check_compatibility_none_mode(self):
        """Test that NONE mode skips compatibility checking."""
        validator = SchemaValidator(CompatibilityMode.NONE)

        old_schema = {"type": "record", "name": "User", "fields": []}
        new_schema = {"type": "enum", "name": "User", "symbols": []}

        # Should pass even though schemas are incompatible
        assert validator.check_compatibility(new_schema, old_schema) is True

    def test_check_compatibility_with_override_mode(self):
        """Test check_compatibility with mode override."""
        validator = SchemaValidator(CompatibilityMode.BACKWARD)

        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        # Override to NONE mode for this check
        assert validator.check_compatibility(
            schema, schema, mode=CompatibilityMode.NONE
        ) is True
