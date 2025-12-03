"""Gzip middleware for FastPubSub."""

import gzip
from typing import Any

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware


# V2: Middlewares must can have args/kwargs
class GZipMiddleware(BaseMiddleware):
    """A middleware for compressing and decompressing messages using gzip."""

    async def on_message(self, message: Message) -> Any:
        """Decompresses a message.

        Args:
            message: The message to decompress.
        """
        if message.attributes and message.attributes.get("Content-Encoding") == "gzip":
            decompressed_data = gzip.decompress(data=message.data)
            new_message = Message(
                id=message.id,
                size=message.size,
                data=decompressed_data,
                attributes=message.attributes,
                delivery_attempt=message.delivery_attempt,
                project_id=message.project_id,
                topic_name=message.topic_name,
                subscriber_name=message.subscriber_name,
            )
            return await super().on_message(new_message)

        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        """Compresses a message.

        Args:
            data: The message data to compress.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
        """
        if not attributes:
            attributes = {}

        attributes["Content-Encoding"] = "gzip"
        compressed_data = gzip.compress(data=data)
        return await super().on_publish(compressed_data, ordering_key, attributes)
