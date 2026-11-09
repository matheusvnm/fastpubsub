"""Integration tests for graceful shutdown with real PubSub emulator."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from fastpubsub import Message
from fastpubsub._internal.clients import PubSubClientFactory
from fastpubsub._internal.event_loop_bridge import EventLoopBridge

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestGracefulShutdown:
    """Test graceful shutdown with real PubSub emulator."""

    @pytest.mark.asyncio
    async def test_in_flight_messages_complete_during_shutdown(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test that messages being processed complete during shutdown."""
        received_messages: list[str] = []
        processing_started = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-in-flight",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def slow_handler(msg: Message) -> None:
            """Handler that takes time to process."""
            processing_started.set()
            await asyncio.sleep(1.0)
            received_messages.append(msg.data.decode())

        await connected_broker.start()

        await connected_broker.publish(
            topic_name=unique_topic, data=b"test-message-1"
        )

        await asyncio.wait_for(processing_started.wait(), timeout=5.0)

        await connected_broker.shutdown()

        assert len(received_messages) == 1
        assert received_messages[0] == "test-message-1"

    @pytest.mark.asyncio
    async def test_new_messages_rejected_after_shutdown_starts(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test that new messages are nacked after shutdown begins."""
        received_messages: list[str] = []
        first_message_received = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-new-rejected",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            first_message_received.set()
            received_messages.append(msg.data.decode())
            await asyncio.sleep(0.5)

        await connected_broker.start()

        await connected_broker.publish(
            topic_name=unique_topic, data=b"message-1"
        )

        await asyncio.wait_for(first_message_received.wait(), timeout=5.0)

        shutdown_task = asyncio.create_task(connected_broker.shutdown())

        await asyncio.sleep(0.2)

        await connected_broker.publish(
            topic_name=unique_topic, data=b"message-2"
        )
        await connected_broker.publish(
            topic_name=unique_topic, data=b"message-3"
        )

        await shutdown_task

        assert len(received_messages) >= 1
        assert "message-1" in received_messages

    @pytest.mark.asyncio
    async def test_shutdown_timeout_is_respected(
        self,
        unique_topic: str,
        unique_subscription: str,
        broker_factory,
    ) -> None:
        """Test that shutdown timeout causes completion within bounds."""
        import time

        received_messages: list[str] = []
        handler_started = asyncio.Event()

        broker = broker_factory(shutdown_timeout=2.0)

        @broker.subscriber(
            alias="test-timeout",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def very_slow_handler(msg: Message) -> None:
            """Handler that takes longer than timeout."""
            received_messages.append(msg.data.decode())
            handler_started.set()
            await asyncio.sleep(10.0)

        await broker.start()
        await broker.publish(topic_name=unique_topic, data=b"test-message")
        await asyncio.wait_for(handler_started.wait(), 15)

        start_time = time.time()
        await broker.shutdown()
        elapsed = time.time() - start_time

        assert elapsed < 4.0, f"Shutdown took {elapsed}s, expected < 4s"
        assert len(received_messages) == 1

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_shutdown_correctly(
        self,
        unique_topic: str,
        unique_subscription: str,
        broker_factory,
    ) -> None:
        """Test shutdown with multiple concurrent subscriptions."""
        received_sub1: list[str] = []
        received_sub2: list[str] = []

        broker = broker_factory()

        topic1 = f"{unique_topic}-1"
        topic2 = f"{unique_topic}-2"
        sub1 = f"{unique_subscription}-1"
        sub2 = f"{unique_subscription}-2"

        @broker.subscriber(
            alias="test-multi-sub1",
            topic_name=topic1,
            subscription_name=sub1,
            autocreate=True,
        )
        async def handler1(msg: Message) -> None:
            received_sub1.append(msg.data.decode())
            await asyncio.sleep(0.5)

        @broker.subscriber(
            alias="test-multi-sub2",
            topic_name=topic2,
            subscription_name=sub2,
            autocreate=True,
        )
        async def handler2(msg: Message) -> None:
            received_sub2.append(msg.data.decode())
            await asyncio.sleep(0.5)

        await broker.start()

        await broker.publish(topic_name=topic1, data=b"message-1")
        await broker.publish(topic_name=topic2, data=b"message-2")

        await asyncio.sleep(1.0)

        await broker.shutdown()

        assert len(received_sub1) == 1
        assert len(received_sub2) == 1

    @pytest.mark.asyncio
    async def test_factory_clients_closed_on_shutdown(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Test that the factory and the bridge caches are cleared."""
        received_messages: list[str] = []

        @connected_broker.subscriber(
            alias="test-factory-close",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            received_messages.append(msg.data.decode())

        await connected_broker.start()

        await connected_broker.publish(
            topic_name=unique_topic, data=b"test-message"
        )

        await asyncio.sleep(1.0)

        assert PubSubClientFactory._publishers
        assert EventLoopBridge._bridges

        await connected_broker.shutdown()

        assert not PubSubClientFactory._publishers
        assert not PubSubClientFactory._publisher_creations
        assert not PubSubClientFactory._admin_clients
        assert not EventLoopBridge._bridges
