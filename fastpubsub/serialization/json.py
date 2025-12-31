"""JSON serializer for FastPubSub messages.

This module provides the JsonSerializer for strict JSON encoding/decoding.
Use this when you want to enforce JSON format and get clear errors on
invalid data.
"""

import json
from typing import Any

from fastpubsub.serialization.base import Serializer
from fastpubsub.serialization.exceptions import DecodingError, EncodingError


class JsonSerializer(Serializer):
    """Serializer for JSON content.

    Serializes Python objects as JSON bytes and vice-versa.
    """

    SUPPORTED_CONTENT_TYPES = frozenset(
        {
            "application/json",
            "text/json",
            "application/x-json",
        }
    )

    def decode(self, data: bytes) -> Any:
        """Decode JSON bytes into a Python object.

        Args:
            data: Raw bytes containing JSON.

        Returns:
            Decoded Python object (dict, list, str, int, float, bool, None).

        Raises:
            DecodingError: If data is not valid JSON.
        """
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            raise DecodingError(
                f"Invalid JSON: {e.msg} at line {e.lineno} column {e.colno}",
                data=data,
                content_type="application/json",
                decoder=type(self),
            ) from e
        except UnicodeDecodeError as e:
            raise DecodingError(
                f"Invalid UTF-8 encoding: {e.reason}",
                data=data,
                content_type="application/json",
                decoder=type(self),
            ) from e

    def encode(self, data: Any) -> tuple[bytes, str]:
        """Encode a Python object as JSON bytes.

        Args:
            data: Python object to encode.

        Returns:
            Tuple of (JSON bytes, "application/json").

        Raises:
            EncodingError: If data cannot be serialized to JSON.
        """
        try:
            if hasattr(data, "model_dump_json"):
                # Pydantic model
                return data.model_dump_json().encode(), "application/json"

            json_str = json.dumps(data, separators=(",", ":"))
            return json_str.encode(), "application/json"
        except (TypeError, ValueError) as e:
            raise EncodingError(
                f"Cannot JSON encode {type(data).__name__}: {e}",
                data=data,
                encoder=type(self),
            ) from e

    def supports(self, content_type: str) -> bool:
        """Check if this encoder supports the given content-type.

        Args:
            content_type: MIME content-type string.

        Returns:
            True if content-type is a JSON type.
        """
        base_type = content_type.split(";")[0].strip().lower()
        return base_type in self.SUPPORTED_CONTENT_TYPES
