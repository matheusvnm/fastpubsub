"""Default serializer with JSON and raw bytes support.

This module provides the DefaultSerializer, which is the fallback serializer
used when no specific serializer is configured. It automatically handles
JSON, plain text, and raw bytes based on data type.
"""

import json
from typing import Any

from pydantic import BaseModel

from fastpubsub.serialization.base import Serializer
from fastpubsub.serialization.exceptions import EncodingError


class DefaultSerializer(Serializer):
    """Default serializer with JSON-first, fallback to str/bytes.

    This serializer attempts to encode/decode data as JSON first. For strings,
    it encodes/decode as text/plain. For bytes, it passes through unchanged.

    Use this serializer when:
    - Content-type is not specified
    - You want automatic format detection based on data type

    Important:
    - When publishing messages this encoder will fail if the data is not
    one of the following types: bytes, str, list, dict or BaseModel.
    """

    def decode(self, data: bytes) -> Any:
        """Decode bytes, trying JSON first then falling back to raw.

        Args:
            data: Raw bytes to decode.

        Returns:
            Decoded JSON object if valid JSON, otherwise raw bytes.
        """
        try:
            return json.loads(data)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return data

    def encode(self, data: Any) -> tuple[bytes, str]:
        """Encode data, trying JSON first then falling back to other formats.

        Args:
            data: Python object to encode.

        Returns:
            Tuple of (encoded bytes, content-type string).

        Raises:
            EncodingError: If encoding fails.
        """
        # Try JSON first for dicts/lists/Pydantic
        if isinstance(data, dict) or isinstance(data, list):
            return json.dumps(data, separators=(",", ":")).encode(), "application/json"

        if isinstance(data, BaseModel):
            return data.model_dump_json().encode(), "application/json"

        if isinstance(data, str):
            return data.encode("utf-8"), "text/plain"

        if isinstance(data, bytes):
            return data, "application/octet-stream"

        try:
            return json.dumps(data, separators=(",", ":")).encode(), "application/json"
        except (TypeError, ValueError) as e:
            raise EncodingError(
                f"Cannot encode {type(data).__name__}: {e}",
                data=data,
                encoder=type(self),
            ) from e

    def supports(self, content_type: str) -> bool:
        """DefaultSerializer supports all content types as fallback.

        Args:
            content_type: MIME content-type string.

        Returns:
            Always True - this is the default serializer.
        """
        return True
