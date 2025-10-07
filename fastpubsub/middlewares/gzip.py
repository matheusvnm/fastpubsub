import gzip
from typing import Any

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware


# V2: Middlewares must can have args/kwargs
class GZipMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        if message.attributes and message.attributes.get("Content-Encoding") == "gzip":
            decompressed_data = gzip.decompress(data=message.data)
            message = Message(
                id=message.id,
                size=message.size,
                ack_id=message.ack_id,
                data=decompressed_data,
                attributes=message.attributes,
                delivery_attempt=message.delivery_attempt,
            )

        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        if not attributes:
            attributes = {}

        attributes["Content-Encoding"] = "gzip"
        compressed_data = gzip.compress(data=data)
        return await super().on_publish(compressed_data, ordering_key, attributes)
