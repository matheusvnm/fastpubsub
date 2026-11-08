import threading
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from fastpubsub.datastructures import Message
from fastpubsub.middlewares.di import (
    HandleMessageSerializerMiddleware,
    PublishMessageSerializerMiddleware,
)
from fastpubsub.transport import NotFound


@pytest.fixture
def message() -> Message:
    return Message(
        id="1",
        data=b"payload",
        size=7,
        attributes={},
        delivery_attempt=1,
        project_id="project",
        topic_name="topic",
        subscriber_name="subscriber",
    )


@pytest.fixture
def pubsub_client() -> Generator[MagicMock]:
    client = MagicMock()
    client.create_topic = AsyncMock()
    client.publish = AsyncMock()
    with patch("fastpubsub.middlewares.di.PubSubClient", return_value=client):
        yield client


class TestHandleMessageSerializerMiddleware:
    @pytest.mark.asyncio
    async def test_async_handler_runs_on_event_loop_thread(
        self, message: Message
    ):
        async def handler(_: Message) -> int:
            return threading.get_ident()

        middleware = HandleMessageSerializerMiddleware(None, handler)

        assert await middleware.on_message(message) == threading.get_ident()

    @pytest.mark.asyncio
    async def test_sync_handler_runs_in_worker_thread(self, message: Message):
        def handler(_: Message) -> int:
            return threading.get_ident()

        middleware = HandleMessageSerializerMiddleware(None, handler)

        assert await middleware.on_message(message) != threading.get_ident()

    @pytest.mark.asyncio
    async def test_sync_handler_exception_reaches_caller(
        self, message: Message
    ):
        def handler(_: Message) -> None:
            raise ValueError("handler failed")

        middleware = HandleMessageSerializerMiddleware(None, handler)

        with pytest.raises(ValueError, match="handler failed"):
            await middleware.on_message(message)


class TestPublishMessageSerializerMiddleware:
    @pytest.mark.asyncio
    async def test_autocreate_creates_topic_once(
        self, pubsub_client: MagicMock
    ):
        middleware = PublishMessageSerializerMiddleware(
            None, project_id="project", topic_name=f"topic-{uuid4()}"
        )

        await middleware.on_publish(b"first", "", None)
        await middleware.on_publish(b"second", "", None)

        pubsub_client.create_topic.assert_awaited_once()
        assert pubsub_client.publish.await_count == 2

    @pytest.mark.asyncio
    async def test_without_autocreate_does_not_create_topic(
        self, pubsub_client: MagicMock
    ):
        middleware = PublishMessageSerializerMiddleware(
            None,
            project_id="project",
            topic_name=f"topic-{uuid4()}",
            autocreate=False,
        )

        await middleware.on_publish(b"data", "", None)

        pubsub_client.create_topic.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_topic_not_found_creates_topic_again(
        self, pubsub_client: MagicMock
    ):
        middleware = PublishMessageSerializerMiddleware(
            None, project_id="project", topic_name=f"topic-{uuid4()}"
        )
        pubsub_client.publish.side_effect = [NotFound("deleted"), None]

        with pytest.raises(NotFound):
            await middleware.on_publish(b"first", "", None)
        await middleware.on_publish(b"second", "", None)

        assert pubsub_client.create_topic.await_count == 2
