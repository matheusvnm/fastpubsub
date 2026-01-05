"""Protocol definitions for serialization components."""

from typing import Any

from pydantic.dataclasses import dataclass


@dataclass
class Serializer:
    """Protocol for message body serializer.

    A serializer has two main functionalities:
        1. Convert raw bytes into Python objects (dict, list, str, etc.).
        2. Convert Python objects into bytes for sending.
        3. Check if a given content-type can be decoded/encoded
    """

    CONTENT_TYPE_KEY = "content-type"

    def decode(self, data: bytes) -> Any:
        """Decode raw bytes into a Python object.

        Args:
            data: Raw bytes to decode.

        Returns:
            Decoded Python object (dict, list, str, etc.).

        Raises:
            DecodingError: If decoding fails.
        """
        raise NotImplementedError

    def encode(self, data: Any) -> tuple[bytes, str]:
        """Encode a Python object into bytes.

        Args:
            data: Python object to encode.

        Returns:
            Tuple of (encoded bytes, content-type string).

        Raises:
            EncodingError: If encoding fails.
        """
        raise NotImplementedError

    def supports(self, content_type: str) -> bool:
        """Check if this encoder/decoder supports the given content_type.

        Args:
            content_type: MIME content-type string.

        Returns:
            True if this decoder can handle the content-type.
        """
        raise NotImplementedError
