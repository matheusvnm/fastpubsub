"""Integration tests for message consumption."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from fastpubsub import Message, PullMessage
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestMessageConsumption:
    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_consume_single_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[PullMessage] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-consumer",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg)

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data="Hello, PubSub!",
            )
            result = await asyncio.wait_for(
                received.get(),
                timeout=self.timeout,
            )
            assert isinstance(result, PullMessage)
            assert result.data == b"Hello, PubSub!"
            assert result.subscriber_name == "handler"
            assert result.delivery_attempt == 1
            assert result.topic_name == unique_topic
            # content-type is now automatically added by the serializer
            assert result.attributes.get("content-type") == "text/plain"

    @pytest.mark.asyncio
    async def test_consume_multiple_messages(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: list[str] = []
        event = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-multi-consumer",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            received.append(msg.data.decode())
            if len(received) == 3:
                event.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="Message 1")
            await connected_broker.publish(topic_name=unique_topic, data="Message 2")
            await connected_broker.publish(topic_name=unique_topic, data="Message 3")

            await asyncio.wait_for(event.wait(), timeout=self.timeout)

            assert len(received) == 3
            assert "Message 1" in received
            assert "Message 2" in received
            assert "Message 3" in received
