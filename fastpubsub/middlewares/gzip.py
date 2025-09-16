# TODO: Implementar o middleware de gzip de mensagens.


import gzip
from typing import Any

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware


class GzipMiddleware(BaseMiddleware):
    # TODO: Ajustar middlewares para permitir args/kwargs
    def __init__(self, compress_level: int = 9):
        self.compress_level = compress_level

    async def on_message(self, message: Message):
        if message.attributes and message.attributes.get("Content-Encoding") == "gzip":
            decompressed_data = gzip.decompress(
                data=message.data, compresslevel=self.compress_level
            )
            new_message = Message(
                id=message.id,
                size=message.size,
                data=decompressed_data,
                attributes=message.attributes,
                delivery_attempt=message.delivery_attempt,
            )

            return await self.next_call.on_message(new_message)

        return await self.next_call.on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        if not attributes:
            attributes = {}

        attributes["Content-Encoding"] = "gzip"
        compressed_data = gzip.compress(data=data, compresslevel=self.compress_level)
        return await self.next_call.on_publish(compressed_data, ordering_key, attributes)
