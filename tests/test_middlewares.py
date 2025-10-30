# TEST: GZIP (ON/PUBLISH/MESSAGE)
import gzip
from typing import Any

import pytest

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.middlewares.gzip import GZipMiddleware


class MockMiddleware(BaseMiddleware):
    def __init__(self):
        self.received_message = None
        self.next_call = None

    async def on_message(self, message: Message) -> Any:
        self.received_message = message
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str | None, attributes: dict[str, str] | None
    ):
        self.published_message, self.published_attributes = data, attributes
        return await super().on_publish(data, ordering_key, attributes)


class TestGZipMiddleware:
    @pytest.mark.asyncio
    async def test_message_compression(self):
        mock_middleware = MockMiddleware()
        middleware = GZipMiddleware(next_call=mock_middleware)

        data = b"some_reality_big_message_string_with_data"
        await middleware.on_publish(data, "", None)
        assert gzip.decompress(mock_middleware.published_message) == data

        message = Message(
            id="2123",
            size=3,
            data=mock_middleware.published_message,
            attributes=mock_middleware.published_attributes,
            delivery_attempt=0,
        )
        await middleware.on_message(message=message)
        assert mock_middleware.received_message.data == data
