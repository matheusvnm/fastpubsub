"""Title: Testing Middlewares

Demonstrates how to test middlewares in isolation and with integration tests.
"""

import logging
from typing import Any
from unittest.mock import AsyncMock

import pytest

from fastpubsub import BaseMiddleware, Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(f"Processing message {message.id}")
        try:
            return await super().on_message(message)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            raise


# --8<-- [start:unit_test_middleware]
class TestLoggingMiddleware:
    @pytest.fixture
    def middleware(self):
        middleware = LoggingMiddleware()
        middleware._call_next = AsyncMock(return_value=None)
        return middleware

    @pytest.fixture
    def sample_message(self):
        return Message(
            id="test-123",
            size=100,
            data=b'{"test": "data"}',
            attributes={},
            delivery_attempt=1,
            project_id="test-project",
            topic_name="test-topic",
            subscriber_name="test-subscriber",
        )

    @pytest.mark.asyncio
    async def test_logs_message_processing(self, middleware, sample_message, caplog):
        await middleware.on_message(sample_message)

        assert "Processing message test-123" in caplog.text
        middleware._call_next.assert_called_once()

    @pytest.mark.asyncio
    async def test_logs_errors(self, middleware, sample_message, caplog):
        middleware._call_next.side_effect = ValueError("Test error")

        with pytest.raises(ValueError):
            await middleware.on_message(sample_message)

        assert "Error processing message" in caplog.text
# --8<-- [end:unit_test_middleware]


# --8<-- [start:integration_test_middleware]
@pytest.mark.asyncio
async def test_middleware_integration():
    processed_messages = []

    class TrackingMiddleware(BaseMiddleware):
        async def on_message(self, message: Message):
            processed_messages.append(message.id)
            return await super().on_message(message)

    broker = PubSubBroker(project_id="test")
    broker.include_middleware(TrackingMiddleware)

    @broker.subscriber(
        alias="test-handler",
        topic_name="test-topic",
        subscription_name="test-subscription",
    )
    async def handle(message: Message):
        pass

    async with PubSubTestClient(broker) as client:
        await client.publish({"data": "test"}, topic="test-topic")

    assert len(processed_messages) == 1
# --8<-- [end:integration_test_middleware]
