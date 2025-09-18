import gzip
from typing import Any, Union

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import HandleMessageCommand, PublishMessageCommand


class GzipMiddleware(BaseMiddleware):
    # TODO: Ajustar middlewares para permitir args/kwargs
    def __init__(
        self,
        next_call: Union[
            "BaseMiddleware", "PublishMessageCommand", "HandleMessageCommand"
        ],  # TODO: Transformar em parametro tipado
        compress_level: int = 9,
    ):
        super().__init__(next_call)
        self.compress_level = compress_level

    async def on_message(self, message: Message):
        if message.attributes and message.attributes.get("Content-Encoding") == "gzip":
            decompressed_data = gzip.decompress(data=message.data)
            new_message = Message(
                id=message.id,
                size=message.size,
                data=decompressed_data,
                attributes=message.attributes,
                delivery_attempt=message.delivery_attempt,
            )

            return await super().on_message(new_message)

        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        if not attributes:
            attributes = {}

        attributes["Content-Encoding"] = "gzip"
        compressed_data = gzip.compress(data=data, compresslevel=self.compress_level)
        return await super().on_publish(compressed_data, ordering_key, attributes)
