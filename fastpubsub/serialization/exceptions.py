"""Custom exceptions for serialization errors."""

from typing import Any

from fastpubsub.serialization.base import Serializer


class SerializationError(Exception):
    """Base exception for all serialization errors."""

    pass


class DecodingError(SerializationError):
    """Raised when message decoding fails.

    Attributes:
        data: The raw data that failed to decode.
        content_type: The content-type that was attempted.
        decoder: The decoder class that failed.
    """

    def __init__(
        self,
        message: str,
        *,
        data: bytes | None = None,
        content_type: str | None = None,
        decoder: type[Serializer] | None = None,
    ) -> None:
        """Initialize the DecodingError.

        Args:
            message: The error message.
            data: The raw data that failed to decode.
            content_type: The content-type that was attempted.
            decoder: The decoder class that failed.
        """
        self.data = data
        self.content_type = content_type
        self.decoder = decoder
        super().__init__(message)

    def __str__(self) -> str:
        """Return a formatted error message with context."""
        parts = [super().__str__()]
        if self.content_type:
            parts.append(f"Content-Type: {self.content_type}")
        if self.decoder:
            parts.append(f"Decoder: {self.decoder.__name__}")
        if self.data:
            preview = self.data[:100].decode("utf-8", errors="replace")
            parts.append(f"Data preview: {preview}...")
        return " | ".join(parts)


class ValidationError(SerializationError):
    """Raised when field validation fails.

    Attributes:
        field_name: Name of the field that failed validation.
        field_type: Expected type of the field.
        value: The value that failed validation.
        constraints: The constraints that were violated.
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        field_type: type | None = None,
        value: Any = None,
        constraints: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the ValidationError.

        Args:
            message: The error message.
            field_name: Name of the field that failed validation.
            field_type: Expected type of the field.
            value: The value that failed validation.
            constraints: The constraints that were violated.
        """
        self.field_name = field_name
        self.field_type = field_type
        self.value = value
        self.constraints = constraints or {}
        super().__init__(message)

    def __str__(self) -> str:
        """Return a formatted error message with context."""
        parts = [super().__str__()]
        if self.field_name:
            parts.append(f"Field: {self.field_name}")
        if self.field_type:
            parts.append(f"Expected type: {self.field_type.__name__}")
        if self.value is not None:
            parts.append(f"Got value: {self.value!r}")
        if self.constraints:
            constraints_str = ", ".join(f"{k}={v}" for k, v in self.constraints.items())
            parts.append(f"Constraints: {constraints_str}")
        return " | ".join(parts)


class EncodingError(SerializationError):
    """Raised when message encoding fails.

    Attributes:
        data: The data that failed to encode.
        encoder: The encoder class that failed.
    """

    def __init__(
        self,
        message: str,
        *,
        data: Any = None,
        encoder: type[Serializer] | None = None,
    ) -> None:
        """Initialize the EncodingError.

        Args:
            message: The error message.
            data: The data that failed to encode.
            encoder: The encoder class that failed.
        """
        self.data = data
        self.encoder = encoder
        super().__init__(message)

    def __str__(self) -> str:
        """Return a formatted error message with context."""
        parts = [super().__str__()]
        if self.encoder:
            parts.append(f"Encoder: {self.encoder.__name__}")
        if self.data is not None:
            parts.append(f"Data type: {type(self.data).__name__}")
        return " | ".join(parts)
