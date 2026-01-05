"""Tests for serializers (DefaultSerializer and JsonSerializer)."""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel

from fastpubsub.serialization.default import DefaultSerializer
from fastpubsub.serialization.exceptions import DecodingError, EncodingError
from fastpubsub.serialization.json import JsonSerializer


class SampleModel(BaseModel):
    """Sample Pydantic model for testing."""

    name: str
    value: int


class TestDefaultSerializer:
    """Tests for DefaultSerializer."""

    @pytest.fixture
    def serializer(self) -> DefaultSerializer:
        """Create a DefaultSerializer instance."""
        return DefaultSerializer()

    # Encoding tests

    def test_encode_dict(self, serializer: DefaultSerializer):
        """Test encoding a dictionary."""
        data = {"key": "value", "number": 42}

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_list(self, serializer: DefaultSerializer):
        """Test encoding a list."""
        data = [1, 2, 3, "four"]

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_string(self, serializer: DefaultSerializer):
        """Test encoding a string."""
        data = "hello world"

        encoded, content_type = serializer.encode(data)

        assert content_type == "text/plain"
        assert encoded == b"hello world"

    def test_encode_bytes(self, serializer: DefaultSerializer):
        """Test encoding bytes."""
        data = b"\x00\x01\x02\x03"

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/octet-stream"
        assert encoded == data

    def test_encode_pydantic_model(self, serializer: DefaultSerializer):
        """Test encoding a Pydantic model."""
        data = SampleModel(name="test", value=123)

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        decoded = json.loads(encoded)
        assert decoded == {"name": "test", "value": 123}

    def test_encode_nested_dict(self, serializer: DefaultSerializer):
        """Test encoding nested dictionaries."""
        data = {"outer": {"inner": {"deep": "value"}}}

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_int(self, serializer: DefaultSerializer):
        """Test encoding an integer (falls back to JSON)."""
        data = 42

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == 42

    def test_encode_float(self, serializer: DefaultSerializer):
        """Test encoding a float (falls back to JSON)."""
        data = 3.14159

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_bool(self, serializer: DefaultSerializer):
        """Test encoding a boolean (falls back to JSON)."""
        data = True

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) is True

    def test_encode_none(self, serializer: DefaultSerializer):
        """Test encoding None (falls back to JSON)."""
        data = None

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) is None

    def test_encode_unencodable_raises_error(self, serializer: DefaultSerializer):
        """Test encoding unencodable data raises EncodingError."""

        class Unencodable:
            pass

        with pytest.raises(EncodingError):
            serializer.encode(Unencodable())

    # Decoding tests

    def test_decode_json_dict(self, serializer: DefaultSerializer):
        """Test decoding JSON dictionary."""
        data = b'{"key": "value"}'

        decoded = serializer.decode(data)

        assert decoded == {"key": "value"}

    def test_decode_json_list(self, serializer: DefaultSerializer):
        """Test decoding JSON list."""
        data = b"[1, 2, 3]"

        decoded = serializer.decode(data)

        assert decoded == [1, 2, 3]

    def test_decode_json_string(self, serializer: DefaultSerializer):
        """Test decoding JSON string."""
        data = b'"hello"'

        decoded = serializer.decode(data)

        assert decoded == "hello"

    def test_decode_invalid_json_returns_bytes(self, serializer: DefaultSerializer):
        """Test decoding invalid JSON returns raw bytes."""
        data = b"not valid json"

        decoded = serializer.decode(data)

        assert decoded == data

    def test_decode_binary_data_returns_bytes(self, serializer: DefaultSerializer):
        """Test decoding binary data returns raw bytes."""
        data = b"\x00\x01\x02\x03"

        decoded = serializer.decode(data)

        assert decoded == data

    # supports() tests

    def test_supports_any_content_type(self, serializer: DefaultSerializer):
        """Test that DefaultSerializer supports any content type."""
        assert serializer.supports("application/json") is True
        assert serializer.supports("text/plain") is True
        assert serializer.supports("application/octet-stream") is True
        assert serializer.supports("custom/type") is True
        assert serializer.supports("") is True


class TestJsonSerializer:
    """Tests for JsonSerializer."""

    @pytest.fixture
    def serializer(self) -> JsonSerializer:
        """Create a JsonSerializer instance."""
        return JsonSerializer()

    # Encoding tests

    def test_encode_dict(self, serializer: JsonSerializer):
        """Test encoding a dictionary."""
        data = {"key": "value", "number": 42}

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_list(self, serializer: JsonSerializer):
        """Test encoding a list."""
        data = [1, 2, 3, "four"]

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == data

    def test_encode_string(self, serializer: JsonSerializer):
        """Test encoding a string (JSON encoded)."""
        data = "hello"

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == "hello"

    def test_encode_pydantic_model(self, serializer: JsonSerializer):
        """Test encoding a Pydantic model."""
        data = SampleModel(name="test", value=456)

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        decoded = json.loads(encoded)
        assert decoded == {"name": "test", "value": 456}

    def test_encode_int(self, serializer: JsonSerializer):
        """Test encoding an integer."""
        data = 99

        encoded, content_type = serializer.encode(data)

        assert content_type == "application/json"
        assert json.loads(encoded) == 99

    def test_encode_unencodable_raises_error(self, serializer: JsonSerializer):
        """Test encoding unencodable data raises EncodingError."""

        class Unencodable:
            pass

        with pytest.raises(EncodingError) as exc_info:
            serializer.encode(Unencodable())

        assert "Cannot JSON encode" in str(exc_info.value)

    def test_encode_bytes_raises_error(self, serializer: JsonSerializer):
        """Test encoding bytes raises EncodingError (not JSON serializable)."""
        data = b"\x00\x01\x02"

        with pytest.raises(EncodingError):
            serializer.encode(data)

    # Decoding tests

    def test_decode_valid_json(self, serializer: JsonSerializer):
        """Test decoding valid JSON."""
        data = b'{"key": "value"}'

        decoded = serializer.decode(data)

        assert decoded == {"key": "value"}

    def test_decode_json_list(self, serializer: JsonSerializer):
        """Test decoding JSON list."""
        data = b"[1, 2, 3]"

        decoded = serializer.decode(data)

        assert decoded == [1, 2, 3]

    def test_decode_invalid_json_raises_error(self, serializer: JsonSerializer):
        """Test decoding invalid JSON raises DecodingError."""
        data = b"not valid json"

        with pytest.raises(DecodingError) as exc_info:
            serializer.decode(data)

        assert "Invalid JSON" in str(exc_info.value)

    def test_decode_invalid_utf8_raises_error(self, serializer: JsonSerializer):
        """Test decoding invalid UTF-8 raises DecodingError."""
        # Invalid UTF-8 sequence
        data = b"\xff\xfe"

        with pytest.raises(DecodingError) as exc_info:
            serializer.decode(data)

        assert "Invalid" in str(exc_info.value)

    # supports() tests

    def test_supports_application_json(self, serializer: JsonSerializer):
        """Test supports application/json."""
        assert serializer.supports("application/json") is True

    def test_supports_text_json(self, serializer: JsonSerializer):
        """Test supports text/json."""
        assert serializer.supports("text/json") is True

    def test_supports_application_x_json(self, serializer: JsonSerializer):
        """Test supports application/x-json."""
        assert serializer.supports("application/x-json") is True

    def test_supports_json_with_charset(self, serializer: JsonSerializer):
        """Test supports JSON content-type with charset parameter."""
        assert serializer.supports("application/json; charset=utf-8") is True

    def test_does_not_support_text_plain(self, serializer: JsonSerializer):
        """Test does not support text/plain."""
        assert serializer.supports("text/plain") is False

    def test_does_not_support_octet_stream(self, serializer: JsonSerializer):
        """Test does not support application/octet-stream."""
        assert serializer.supports("application/octet-stream") is False

    def test_does_not_support_custom_type(self, serializer: JsonSerializer):
        """Test does not support custom types."""
        assert serializer.supports("application/custom") is False

    def test_supports_case_insensitive(self, serializer: JsonSerializer):
        """Test content-type matching is case-insensitive."""
        assert serializer.supports("APPLICATION/JSON") is True
        assert serializer.supports("Application/Json") is True


class TestEncodingError:
    """Tests for EncodingError exception."""

    def test_encoding_error_stores_data(self):
        """Test EncodingError stores the data that failed."""
        data = {"key": "value"}
        error = EncodingError("Failed", data=data, encoder=DefaultSerializer)

        assert error.data == data
        assert error.encoder == DefaultSerializer

    def test_encoding_error_message(self):
        """Test EncodingError message."""
        error = EncodingError("Custom message", data=None, encoder=None)

        assert "Custom message" in str(error)


class TestDecodingError:
    """Tests for DecodingError exception."""

    def test_decoding_error_stores_data(self):
        """Test DecodingError stores the data that failed."""
        data = b"invalid"
        error = DecodingError(
            "Failed", data=data, content_type="application/json", decoder=JsonSerializer
        )

        assert error.data == data
        assert error.content_type == "application/json"
        assert error.decoder == JsonSerializer

    def test_decoding_error_message(self):
        """Test DecodingError message."""
        error = DecodingError("Custom message", data=None, content_type=None, decoder=None)

        assert "Custom message" in str(error)
