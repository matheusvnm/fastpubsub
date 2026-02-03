"""Title: Subscriber-Only and Publisher-Only Middlewares

Demonstrates middlewares that only affect one message direction.
"""

import gzip
import json
from typing import Any

from fastpubsub import BaseMiddleware, Message
from fastpubsub.exceptions import Drop


# --8<-- [start:validation_middleware]
class ValidationMiddleware(BaseMiddleware):
    """Only validates incoming messages."""

    async def on_message(self, message: Message) -> Any:
        if not self._is_valid(message.data):
            raise Drop("Invalid message format")
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        return await super().on_publish(data, ordering_key, attributes)

    def _is_valid(self, data: bytes) -> bool:
        try:
            json.loads(data)
            return True
        except json.JSONDecodeError:
            return False
# --8<-- [end:validation_middleware]


# --8<-- [start:compression_middleware]
class CompressionMiddleware(BaseMiddleware):
    """Only compresses outgoing messages."""

    async def on_message(self, message: Message) -> Any:
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        compressed = gzip.compress(data)
        if attributes is None:
            attributes = {}
        attributes["content-encoding"] = "gzip"
        return await super().on_publish(compressed, ordering_key, attributes)
# --8<-- [end:compression_middleware]
