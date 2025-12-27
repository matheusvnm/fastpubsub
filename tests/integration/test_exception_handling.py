"""Integration tests for exception handling."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from fastpubsub import Message
from fastpubsub.exceptions import Drop, Retry
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:

    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestExceptionHandling:
    """Test Drop and Retry exception handling with real PubSub."""

    timeout: float = 30.0

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_retry_exception_nacks_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test Drop exception results in message NACK and redelivery."""
        max_attempts = 3
        received: asyncio.Queue[int] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-drop",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            ack_deadline_seconds=10,
            max_delivery_attempts=max_attempts,
        )
        async def handler(msg: Message) -> None:
            if msg.delivery_attempt >= max_attempts:
                await received.put(msg.delivery_attempt)
                return

            raise Retry("Intentionally dropping message")

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data="msg",
            )

            call_count = await asyncio.wait_for(
                received.get(),
                timeout=self.timeout,
            )

            assert call_count == max_attempts

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_drop_exception_acks_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test Retry exception results in message ACK (no redelivery)."""
        received: asyncio.Queue[Message] = asyncio.Queue(maxsize=3)

        @connected_broker.subscriber(
            alias="test-retry",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg)
            raise Drop("Retry later via external mechanism")

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data="test-message",
            )

            await asyncio.sleep(7.0)
            assert received.qsize() == 1, "Message should only be processed once"

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_unhandled_exception_nacks_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test unhandled exceptions are caught and message is NACKed."""
        completed = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-unhandled",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            ack_deadline_seconds=10,
        )
        async def handler(msg: Message) -> None:
            if msg.delivery_attempt == 1:
                raise ValueError("Something went wrong!")

            completed.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="test")
            await asyncio.wait_for(completed.wait(), timeout=self.timeout)
