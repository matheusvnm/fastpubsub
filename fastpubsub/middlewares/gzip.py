"""Gzip middleware for FastPubSub."""

import gzip
from typing import Any

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware


class GZipMiddleware(BaseMiddleware):
    """A middleware for compressing and decompressing messages using gzip."""

    def __init__(
        self, next_call: BaseMiddleware, compresslevel: int = 9, mtime: int | float | None = None
    ):
        """Initializes the GZipMiddleware.

        Args:
            next_call: The next call in the chain to call.
            compresslevel: The level of compression used on
                gzip.compress function on a ranges of 0 to 9.
            mtime: The modification time. The modification time is
                set to the current time by default.
        """
        super().__init__(next_call)
        self.compresslevel = compresslevel
        self.mtime = mtime

    async def on_message(self, message: Message) -> Any:
        """Decompresses a message.

        Args:
            message: The message to decompress.
        """
        if message.attributes.get("content-encoding", "") == "gzip":
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

        attributes["content-encoding"] = "gzip"
        compressed_data = gzip.compress(
            data=data, compresslevel=self.compresslevel, mtime=self.mtime
        )
        return await super().on_publish(compressed_data, ordering_key, attributes)
