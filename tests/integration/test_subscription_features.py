"""Integration tests for the subscription features of the transport."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

import pytest

from fastpubsub import Message
from fastpubsub._internal.connection import Connection
from fastpubsub.exceptions import Retry
from tests.integration.conftest import delete_resources, managed_broker

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker

needs_pubsub_service = pytest.mark.skipif(
    Connection.is_emulator(),
    reason="the emulator does not apply this subscription setting",
)


@pytest.mark.connected
class TestSubscriptionFeatures:
    timeout: float = 30.0

    @pytest.mark.asyncio
    async def test_filter_delivers_only_matching_messages(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[str] = asyncio.Queue()

        @connected_broker.subscriber(
            alias="test-filter",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            filter_expression='attributes.kind = "order"',
        )
        async def handler(msg: Message) -> None:
            await received.put(msg.data.decode())

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data="ignored",
                attributes={"kind": "user"},
            )
            await connected_broker.publish(
                topic_name=unique_topic,
                data="matched",
                attributes={"kind": "order"},
            )

            first = await asyncio.wait_for(received.get(), self.timeout)
            await asyncio.sleep(2)

        assert first == "matched"
        assert received.empty()

    @pytest.mark.asyncio
    async def test_ordering_key_keeps_the_publish_order(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        message_count = 20
        received: list[int] = []
        all_received = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-ordering",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            enable_message_ordering=True,
        )
        async def handler(msg: Message) -> None:
            received.append(int(msg.data.decode()))
            if len(received) == message_count:
                all_received.set()

        async with managed_broker(connected_broker):
            for index in range(message_count):
                await connected_broker.publish(
                    topic_name=unique_topic,
                    data=str(index),
                    ordering_key="customer-1",
                )
            await asyncio.wait_for(all_received.wait(), self.timeout)

        assert received == list(range(message_count))

    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.timeout(120)
    async def test_dead_letter_topic_receives_the_failed_message(
        self,
        project_id: str,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        max_delivery_attempts = 5
        dead_letter_topic = f"{unique_topic}-dead-letter"
        attempts: list[int] = []
        dead_letters: asyncio.Queue[Message] = asyncio.Queue()

        @connected_broker.subscriber(
            alias="test-dead-letter-source",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            dead_letter_topic=dead_letter_topic,
            max_delivery_attempts=max_delivery_attempts,
            min_backoff_delay_secs=0,
            max_backoff_delay_secs=1,
        )
        async def failing_handler(msg: Message) -> None:
            attempts.append(msg.delivery_attempt)
            raise Retry("always fails")

        @connected_broker.subscriber(
            alias="test-dead-letter-target",
            topic_name=dead_letter_topic,
            subscription_name=f"{unique_subscription}-dead-letter",
            autocreate=True,
        )
        async def dead_letter_handler(msg: Message) -> None:
            await dead_letters.put(msg)

        try:
            async with managed_broker(connected_broker):
                await connected_broker.publish(
                    topic_name=unique_topic, data="poison"
                )
                dead_letter = await asyncio.wait_for(dead_letters.get(), 90)
        finally:
            await asyncio.to_thread(
                delete_resources,
                project_id,
                dead_letter_topic,
                f"{unique_subscription}-dead-letter",
            )
            await asyncio.to_thread(
                delete_resources,
                project_id,
                dead_letter_topic,
                dead_letter_topic,
            )

        assert dead_letter.data == b"poison"
        assert attempts == list(range(1, len(attempts) + 1))
        assert len(attempts) >= max_delivery_attempts

    @needs_pubsub_service
    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.timeout(60)
    async def test_retry_backoff_delays_the_redelivery(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        minimum_backoff_seconds = 5
        delivery_times: list[float] = []
        redelivered = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-backoff",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            min_backoff_delay_secs=minimum_backoff_seconds,
            max_backoff_delay_secs=minimum_backoff_seconds,
        )
        async def handler(msg: Message) -> None:
            delivery_times.append(time.monotonic())
            if len(delivery_times) == 1:
                raise Retry("retry once")
            redelivered.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="x")
            await asyncio.wait_for(redelivered.wait(), 45)

        assert delivery_times[1] - delivery_times[0] >= (
            minimum_backoff_seconds - 1
        )

    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.timeout(90)
    async def test_lease_extension_keeps_a_slow_message_from_redelivery(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        ack_deadline_seconds = 10
        deliveries: list[str] = []
        finished = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-lease",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            ack_deadline_seconds=ack_deadline_seconds,
        )
        async def handler(msg: Message) -> None:
            deliveries.append(msg.id)
            await asyncio.sleep(ack_deadline_seconds * 2)
            finished.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(topic_name=unique_topic, data="x")
            await asyncio.wait_for(finished.wait(), 60)
            await asyncio.sleep(ack_deadline_seconds)

        assert len(deliveries) == 1

    @needs_pubsub_service
    @pytest.mark.asyncio
    @pytest.mark.slow
    @pytest.mark.timeout(90)
    async def test_exactly_once_delivery_acks_each_message_one_time(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        message_count = 10
        deliveries: list[str] = []
        all_received = asyncio.Event()

        @connected_broker.subscriber(
            alias="test-exactly-once",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
            enable_exactly_once_delivery=True,
        )
        async def handler(msg: Message) -> None:
            deliveries.append(msg.data.decode())
            if len(set(deliveries)) == message_count:
                all_received.set()

        async with managed_broker(connected_broker):
            for index in range(message_count):
                await connected_broker.publish(
                    topic_name=unique_topic, data=str(index)
                )
            await asyncio.wait_for(all_received.wait(), 60)
            await asyncio.sleep(15)

        assert sorted(deliveries, key=int) == [
            str(index) for index in range(message_count)
        ]
