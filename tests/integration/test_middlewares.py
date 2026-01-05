"""Integration tests for middlewares."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import pytest

from fastpubsub import Message, PullMessage
from fastpubsub.middlewares.base import BaseMiddleware
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:
    from unittest.mock import MagicMock

    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestMiddlewares:
    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_middleware_chain_execution(
        self,
        mock: MagicMock,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        class LoggingMiddleware(BaseMiddleware):
            async def on_message(self, message: PullMessage) -> Any:
                mock("logging_middleware")
                return await super().on_message(message)

        class ValidationMiddleware(BaseMiddleware):
            async def on_message(self, message: PullMessage) -> Any:
                mock("validation_middleware")
                return await super().on_message(message)

        connected_broker.include_middleware(LoggingMiddleware)
        connected_broker.include_middleware(ValidationMiddleware)

        event = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-middleware-chain",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            mock("handler")
            event.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="test message")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            calls = [call[0][0] for call in mock.call_args_list]
            assert calls == ["logging_middleware", "validation_middleware", "handler"]

    @pytest.mark.asyncio
    async def test_subscriber_specific_middleware(
        self,
        mock: MagicMock,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        class SubscriberMiddleware(BaseMiddleware):
            async def on_message(self, message: Message) -> Any:
                mock("subscriber_middleware")
                return await super().on_message(message)

        event = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-sub-middleware",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            middlewares=[SubscriberMiddleware],
        )
        async def handler(msg: str) -> None:
            mock("handler")
            event.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="test")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            calls = [call[0][0] for call in mock.call_args_list]
            assert "subscriber_middleware" in calls
            assert "handler" in calls

    @pytest.mark.asyncio
    async def test_middleware_modifies_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[dict[str, str]] = asyncio.Queue(maxsize=1)

        class TrackingMiddleware(BaseMiddleware):
            async def on_message(self, message: Message) -> Any:
                await received.put(message.attributes)
                return await super().on_message(message)

            async def on_publish(
                self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
            ) -> Any:
                if not attributes:
                    attributes = {}

                attributes["key"] = "value"
                return await super().on_publish(data, ordering_key, attributes)

        connected_broker.include_middleware(TrackingMiddleware)

        @connected_broker.subscriber(
            alias="test-track-middleware",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: str) -> None:
            pass

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="test")
            attributes = await asyncio.wait_for(received.get(), timeout=self.timeout)

            assert isinstance(attributes, dict)
            assert attributes.get("key") == "value"
