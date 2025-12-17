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

    def test_validate_avro_schema_field_not_dict(self, validator):
        """Test that non-dictionary field raises error."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": ["not a dict"]
        }

        with pytest.raises(SchemaValidationError, match="Field must be a dictionary"):
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

    def test_forward_incompatible_schema_type_change(self, validator):
        """Test that changing schema type is incompatible in forward compatibility."""
        old_schema = {"type": "record", "name": "User", "fields": []}
        new_schema = {"type": "enum", "name": "User", "symbols": []}

        with pytest.raises(SchemaCompatibilityError, match="Schema type changed"):
            validator.check_forward_compatibility(new_schema, old_schema)

    def test_forward_incompatible_field_type_change(self, validator):
        """Test that field type change is incompatible in forward compatibility."""
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "int"}]
        }

        with pytest.raises(SchemaCompatibilityError, match="type changed incompatibly"):
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

    def test_check_compatibility_forward_mode(self):
        """Test check_compatibility with FORWARD mode."""
        validator = SchemaValidator(CompatibilityMode.FORWARD)

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
            "fields": [{"name": "id", "type": "string"}]
        }

        # Forward compatibility: removing field with default should fail
        with pytest.raises(SchemaCompatibilityError):
            validator.check_compatibility(new_schema, old_schema)

    def test_check_compatibility_full_mode(self):
        """Test check_compatibility with FULL mode."""
        validator = SchemaValidator(CompatibilityMode.FULL)

        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        # Same schema should be fully compatible
        assert validator.check_compatibility(schema, schema) is True

    def test_check_compatibility_invalid_mode(self):
        """Test that invalid compatibility mode raises ValueError."""
        validator = SchemaValidator(CompatibilityMode.BACKWARD)

        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        # Create a fake invalid mode
        class InvalidMode:
            value = "INVALID"

        with pytest.raises(ValueError, match="Unknown compatibility mode"):
            validator.check_compatibility(schema, schema, mode=InvalidMode())


class TestSchemaRegistryIntegration:
    """Test Schema Registry integration functionality."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator instance."""
        return SchemaValidator()

    @pytest.fixture
    def validator_with_registry_url(self, mocker):
        """Create a SchemaValidator instance with schema registry URL."""
        # Mock requests.Session to prevent actual HTTP calls
        mock_session = mocker.Mock()
        mocker.patch('requests.Session', return_value=mock_session)
        return SchemaValidator(schema_registry_url="http://localhost:8081")

    @pytest.fixture
    def mock_registry_client(self, mocker):
        """Create a mock Schema Registry client."""
        mock_client = mocker.Mock()
        return mock_client

    def test_get_schema_by_id_success(self, validator, mock_registry_client):
        """Test retrieving schema by ID from registry."""
        schema_dict = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        mock_registry_client.get_schema_by_id.return_value = schema_dict

        # Test that schema_registry_client is used when available
        validator.schema_registry_client = mock_registry_client
        result = validator.get_schema_by_id(123)

        assert result == schema_dict
        mock_registry_client.get_schema_by_id.assert_called_once_with(123)

    def test_get_schema_by_id_not_found(self, validator, mock_registry_client):
        """Test handling of schema not found error."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.get_schema_by_id.side_effect = Exception("Schema not found")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Schema not found"):
            validator.get_schema_by_id(999)

    def test_get_latest_schema_version(self, validator, mock_registry_client):
        """Test retrieving latest schema version for a subject."""
        schema_dict = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}, {"name": "email", "type": "string"}]
        }

        mock_registry_client.get_latest_schema_version.return_value = (2, schema_dict)

        validator.schema_registry_client = mock_registry_client
        version, schema = validator.get_latest_schema_version("user-topic-value")

        assert version == 2
        assert schema == schema_dict
        mock_registry_client.get_latest_schema_version.assert_called_once_with("user-topic-value")

    def test_get_all_schema_versions(self, validator, mock_registry_client):
        """Test retrieving all schema versions for a subject."""
        versions = [
            (1, {"type": "record", "name": "User", "fields": [{"name": "id", "type": "string"}]}),
            (2, {"type": "record", "name": "User", "fields": [{"name": "id", "type": "string"}, {"name": "email", "type": "string", "default": ""}]})
        ]

        mock_registry_client.get_all_versions.return_value = versions

        validator.schema_registry_client = mock_registry_client
        result = validator.get_all_schema_versions("user-topic-value")

        assert len(result) == 2
        assert result[0][0] == 1
        assert result[1][0] == 2

    def test_register_schema(self, validator, mock_registry_client):
        """Test registering a new schema."""
        schema = {
            "type": "record",
            "name": "Product",
            "fields": [{"name": "id", "type": "string"}]
        }

        mock_registry_client.register_schema.return_value = 10

        validator.schema_registry_client = mock_registry_client
        schema_id = validator.register_schema("product-topic-value", schema)

        assert schema_id == 10
        mock_registry_client.register_schema.assert_called_once_with("product-topic-value", schema)

    def test_check_schema_compatibility_with_registry(self, validator, mock_registry_client):
        """Test checking compatibility against registered schemas."""
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

        mock_registry_client.test_compatibility.return_value = True

        validator.schema_registry_client = mock_registry_client
        result = validator.test_compatibility_with_registry("user-topic-value", new_schema)

        assert result is True
        mock_registry_client.test_compatibility.assert_called_once_with("user-topic-value", new_schema)

    def test_check_schema_compatibility_incompatible(self, validator, mock_registry_client):
        """Test detecting incompatible schema."""
        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "different_id", "type": "int"}]
        }

        mock_registry_client.test_compatibility.return_value = False

        validator.schema_registry_client = mock_registry_client
        result = validator.test_compatibility_with_registry("user-topic-value", new_schema)

        assert result is False

    def test_get_schema_versions_diff(self, validator, mock_registry_client):
        """Test getting diff between schema versions."""
        version1_schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        version2_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "email", "type": "string", "default": ""}
            ]
        }

        mock_registry_client.get_schema_by_version.side_effect = [version1_schema, version2_schema]

        validator.schema_registry_client = mock_registry_client
        diff = validator.get_schema_diff("user-topic-value", 1, 2)

        assert "added_fields" in diff
        assert "removed_fields" in diff
        assert "email" in diff["added_fields"]

    def test_list_subjects(self, validator, mock_registry_client):
        """Test listing all subjects in registry."""
        subjects = ["user-topic-value", "product-topic-value", "order-topic-value"]

        mock_registry_client.list_subjects.return_value = subjects

        validator.schema_registry_client = mock_registry_client
        result = validator.list_subjects()

        assert result == subjects
        assert len(result) == 3

    def test_delete_schema_version(self, validator, mock_registry_client):
        """Test deleting a specific schema version."""
        mock_registry_client.delete_schema_version.return_value = True

        validator.schema_registry_client = mock_registry_client
        result = validator.delete_schema_version("user-topic-value", 1)

        assert result is True
        mock_registry_client.delete_schema_version.assert_called_once_with("user-topic-value", 1)

    def test_schema_registry_client_not_configured(self, validator):
        """Test graceful handling when Schema Registry client is not configured."""
        from src.utils.schema_validator import SchemaRegistryError

        # No client configured
        validator.schema_registry_client = None

        with pytest.raises(SchemaRegistryError, match="Schema Registry client not configured"):
            validator.get_schema_by_id(123)

    def test_get_compatibility_mode_from_registry(self, validator, mock_registry_client):
        """Test getting compatibility mode from registry."""
        mock_registry_client.get_compatibility.return_value = "BACKWARD"

        validator.schema_registry_client = mock_registry_client
        mode = validator.get_registry_compatibility_mode("user-topic-value")

        assert mode == CompatibilityMode.BACKWARD

    def test_set_compatibility_mode_in_registry(self, validator, mock_registry_client):
        """Test setting compatibility mode in registry."""
        mock_registry_client.set_compatibility.return_value = True

        validator.schema_registry_client = mock_registry_client
        result = validator.set_registry_compatibility_mode("user-topic-value", CompatibilityMode.FULL)

        assert result is True
        mock_registry_client.set_compatibility.assert_called_once_with("user-topic-value", "FULL")

    def test_simple_registry_client_initialization(self, validator_with_registry_url):
        """Test that SimpleSchemaRegistryClient is initialized with registry URL."""
        assert validator_with_registry_url.schema_registry_client is not None
        assert validator_with_registry_url.schema_registry_url == "http://localhost:8081"

    def test_simple_registry_client_get_schema_by_id(self, mocker):
        """Test SimpleSchemaRegistryClient.get_schema_by_id method."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"schema": {"type": "record", "name": "Test", "fields": []}}
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        result = validator.schema_registry_client.get_schema_by_id(42)

        assert result == {"type": "record", "name": "Test", "fields": []}
        mock_session.get.assert_called_once_with("http://localhost:8081/schemas/ids/42")

    def test_simple_registry_client_get_latest_schema_version(self, mocker):
        """Test SimpleSchemaRegistryClient.get_latest_schema_version method."""
        schema = {"type": "record", "name": "Test", "fields": []}
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"version": 5, "schema": json.dumps(schema)}
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        version, result = validator.schema_registry_client.get_latest_schema_version("test-subject")

        assert version == 5
        assert result == schema
        mock_session.get.assert_called_once_with("http://localhost:8081/subjects/test-subject/versions/latest")

    def test_simple_registry_client_get_all_versions(self, mocker):
        """Test SimpleSchemaRegistryClient.get_all_versions method."""
        schema1 = {"type": "record", "name": "Test", "fields": []}
        schema2 = {"type": "record", "name": "Test", "fields": [{"name": "id", "type": "string"}]}

        mock_versions_response = mocker.Mock()
        mock_versions_response.json.return_value = [1, 2]

        mock_v1_response = mocker.Mock()
        mock_v1_response.json.return_value = {"version": 1, "schema": json.dumps(schema1)}

        mock_v2_response = mocker.Mock()
        mock_v2_response.json.return_value = {"version": 2, "schema": json.dumps(schema2)}

        mock_session = mocker.Mock()
        mock_session.get.side_effect = [mock_versions_response, mock_v1_response, mock_v2_response]
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        results = validator.schema_registry_client.get_all_versions("test-subject")

        assert len(results) == 2
        assert results[0] == (1, schema1)
        assert results[1] == (2, schema2)

    def test_simple_registry_client_register_schema(self, mocker):
        """Test SimpleSchemaRegistryClient.register_schema method."""
        schema = {"type": "record", "name": "Test", "fields": []}
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"id": 123}
        mock_session = mocker.Mock()
        mock_session.post.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        schema_id = validator.schema_registry_client.register_schema("test-subject", schema)

        assert schema_id == 123
        mock_session.post.assert_called_once()

    def test_simple_registry_client_test_compatibility(self, mocker):
        """Test SimpleSchemaRegistryClient.test_compatibility method."""
        schema = {"type": "record", "name": "Test", "fields": []}
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"is_compatible": True}
        mock_session = mocker.Mock()
        mock_session.post.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        result = validator.schema_registry_client.test_compatibility("test-subject", schema)

        assert result is True
        mock_session.post.assert_called_once()

    def test_simple_registry_client_get_schema_by_version(self, mocker):
        """Test SimpleSchemaRegistryClient.get_schema_by_version method."""
        schema = {"type": "record", "name": "Test", "fields": []}
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"schema": json.dumps(schema)}
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        result = validator.schema_registry_client.get_schema_by_version("test-subject", 3)

        assert result == schema
        mock_session.get.assert_called_once_with("http://localhost:8081/subjects/test-subject/versions/3")

    def test_simple_registry_client_list_subjects(self, mocker):
        """Test SimpleSchemaRegistryClient.list_subjects method."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = ["subject1", "subject2", "subject3"]
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        subjects = validator.schema_registry_client.list_subjects()

        assert subjects == ["subject1", "subject2", "subject3"]
        mock_session.get.assert_called_once_with("http://localhost:8081/subjects")

    def test_simple_registry_client_delete_schema_version(self, mocker):
        """Test SimpleSchemaRegistryClient.delete_schema_version method."""
        mock_response = mocker.Mock()
        mock_session = mocker.Mock()
        mock_session.delete.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        result = validator.schema_registry_client.delete_schema_version("test-subject", 2)

        assert result is True
        mock_session.delete.assert_called_once_with("http://localhost:8081/subjects/test-subject/versions/2")

    def test_simple_registry_client_get_compatibility(self, mocker):
        """Test SimpleSchemaRegistryClient.get_compatibility method."""
        mock_response = mocker.Mock()
        mock_response.json.return_value = {"compatibilityLevel": "FULL"}
        mock_session = mocker.Mock()
        mock_session.get.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        level = validator.schema_registry_client.get_compatibility("test-subject")

        assert level == "FULL"
        mock_session.get.assert_called_once_with("http://localhost:8081/config/test-subject")

    def test_simple_registry_client_set_compatibility(self, mocker):
        """Test SimpleSchemaRegistryClient.set_compatibility method."""
        mock_response = mocker.Mock()
        mock_session = mocker.Mock()
        mock_session.put.return_value = mock_response
        mocker.patch('requests.Session', return_value=mock_session)

        validator = SchemaValidator(schema_registry_url="http://localhost:8081")
        result = validator.schema_registry_client.set_compatibility("test-subject", "BACKWARD")

        assert result is True
        mock_session.put.assert_called_once_with("http://localhost:8081/config/test-subject", json={"compatibility": "BACKWARD"})

    def test_get_latest_schema_version_error(self, validator, mock_registry_client):
        """Test error handling when getting latest schema version fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.get_latest_schema_version.side_effect = Exception("Connection failed")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to get latest schema"):
            validator.get_latest_schema_version("test-subject")

    def test_get_all_schema_versions_error(self, validator, mock_registry_client):
        """Test error handling when getting all versions fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.get_all_versions.side_effect = Exception("Network error")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to get versions"):
            validator.get_all_schema_versions("test-subject")

    def test_register_schema_error(self, validator, mock_registry_client):
        """Test error handling when registering schema fails."""
        from src.utils.schema_validator import SchemaRegistryError

        schema = {"type": "record", "name": "Test", "fields": []}
        mock_registry_client.register_schema.side_effect = Exception("Registration failed")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to register schema"):
            validator.register_schema("test-subject", schema)

    def test_test_compatibility_with_registry_error(self, validator, mock_registry_client):
        """Test error handling when compatibility test fails."""
        from src.utils.schema_validator import SchemaRegistryError

        schema = {"type": "record", "name": "Test", "fields": []}
        mock_registry_client.test_compatibility.side_effect = Exception("Test failed")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to test compatibility"):
            validator.test_compatibility_with_registry("test-subject", schema)

    def test_get_schema_diff_error(self, validator, mock_registry_client):
        """Test error handling when getting schema diff fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.get_schema_by_version.side_effect = Exception("Version not found")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to get schema diff"):
            validator.get_schema_diff("test-subject", 1, 2)

    def test_list_subjects_error(self, validator, mock_registry_client):
        """Test error handling when listing subjects fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.list_subjects.side_effect = Exception("Permission denied")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to list subjects"):
            validator.list_subjects()

    def test_delete_schema_version_error(self, validator, mock_registry_client):
        """Test error handling when deleting schema version fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.delete_schema_version.side_effect = Exception("Delete failed")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to delete schema version"):
            validator.delete_schema_version("test-subject", 1)

    def test_get_registry_compatibility_mode_error(self, validator, mock_registry_client):
        """Test error handling when getting compatibility mode fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.get_compatibility.side_effect = Exception("Config error")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to get compatibility mode"):
            validator.get_registry_compatibility_mode("test-subject")

    def test_set_registry_compatibility_mode_error(self, validator, mock_registry_client):
        """Test error handling when setting compatibility mode fails."""
        from src.utils.schema_validator import SchemaRegistryError

        mock_registry_client.set_compatibility.side_effect = Exception("Update failed")

        validator.schema_registry_client = mock_registry_client

        with pytest.raises(SchemaRegistryError, match="Failed to set compatibility mode"):
            validator.set_registry_compatibility_mode("test-subject", CompatibilityMode.FULL)


class TestNamespaceValidation:
    """Test namespace validation (Bug #6)."""

    @pytest.fixture
    def validator(self):
        """Create a SchemaValidator instance in normal mode."""
        return SchemaValidator()

    @pytest.fixture
    def strict_validator(self):
        """Create a SchemaValidator instance in strict mode."""
        return SchemaValidator(strict_mode=True)

    def test_schema_with_namespace_passes(self, validator):
        """Test that schema with namespace passes validation."""
        schema = {
            "type": "record",
            "name": "User",
            "namespace": "com.example.cdc",
            "fields": [{"name": "id", "type": "string"}]
        }

        assert validator.validate_avro_schema(schema) is True

    def test_schema_without_namespace_warns_in_normal_mode(self, validator, caplog):
        """Test that schema without namespace generates warning in normal mode."""
        import logging
        caplog.set_level(logging.WARNING)

        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        # Should pass but generate warning
        assert validator.validate_avro_schema(schema) is True
        assert "missing 'namespace' field" in caplog.text
        assert "Namespaces prevent naming conflicts" in caplog.text

    def test_schema_without_namespace_fails_in_strict_mode(self, strict_validator):
        """Test that schema without namespace raises error in strict mode."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        with pytest.raises(SchemaValidationError) as exc_info:
            strict_validator.validate_avro_schema(schema)

        assert "missing 'namespace' field" in str(exc_info.value)
        assert "Namespaces prevent naming conflicts" in str(exc_info.value)

    def test_namespace_warning_can_be_disabled(self, validator):
        """Test that namespace warning can be disabled."""
        schema = {
            "type": "record",
            "name": "User",
            "fields": [{"name": "id", "type": "string"}]
        }

        # Should pass without warning when warn_missing_namespace=False
        assert validator.validate_avro_schema(schema, warn_missing_namespace=False) is True

    def test_strict_mode_initialization(self, strict_validator):
        """Test that strict mode is properly initialized."""
        assert strict_validator.strict_mode is True

    def test_normal_mode_initialization(self, validator):
        """Test that normal mode is default."""
        assert validator.strict_mode is False

    def test_namespace_in_nested_record(self, validator):
        """Test namespace validation with nested record types."""
        schema = {
            "type": "record",
            "name": "Order",
            "namespace": "com.example.orders",
            "fields": [
                {"name": "id", "type": "string"},
                {
                    "name": "customer",
                    "type": {
                        "type": "record",
                        "name": "Customer",
                        "namespace": "com.example.customers",
                        "fields": [
                            {"name": "customer_id", "type": "string"}
                        ]
                    }
                }
            ]
        }

        assert validator.validate_avro_schema(schema) is True
