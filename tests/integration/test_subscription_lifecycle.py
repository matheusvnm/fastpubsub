"""Integration tests for topic and subscription lifecycle management."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from fastpubsub_core import AdminClient, AlreadyExistsError

from fastpubsub import Message
from fastpubsub._internal.connection import Connection
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestSubscriptionLifecycle:
    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_autocreate_topic_and_subscription(
        self,
        project_id: str,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        @connected_broker.subscriber(
            alias="test-autocreate",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            pass

        async with managed_broker(connected_broker):
            connection = Connection(project_id)
            topic_path = connection.topic_path(unique_topic)
            admin_client = AdminClient(Connection.read_endpoint())

            with pytest.raises(AlreadyExistsError):
                admin_client.create_topic(topic_path)
            with pytest.raises(AlreadyExistsError):
                admin_client.create_subscription(
                    connection.subscription_path(unique_subscription),
                    topic_path,
                    {},
                )

    @pytest.mark.asyncio
    async def test_broker_health_checks(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        @connected_broker.subscriber(
            alias="test-health",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: str) -> None:
            pass

        assert connected_broker.alive() is False
        assert connected_broker.ready() is False

        async with managed_broker(connected_broker):
            assert connected_broker.alive() is True
            assert connected_broker.ready() is True

        assert connected_broker.alive() is False
        assert connected_broker.ready() is False

    @pytest.mark.asyncio
    async def test_multiple_subscribers_same_topic(
        self, unique_topic: str, connected_broker: PubSubBroker
    ) -> None:
        received_by_sub1: list[str] = []
        received_by_sub2: list[str] = []
        event_1 = asyncio.Event()
        event_2 = asyncio.Event()

        @connected_broker.subscriber(
            alias="subscriber-1",
            topic_name=unique_topic,
            subscription_name=f"{unique_topic}-sub-1",
            autocreate=True,
        )
        async def handler1(msg: Message) -> None:
            received_by_sub1.append(msg.data.decode())
            event_1.set()

        @connected_broker.subscriber(
            alias="subscriber-2",
            topic_name=unique_topic,
            subscription_name=f"{unique_topic}-sub-2",
            autocreate=True,
        )
        async def handler2(msg: Message) -> None:
            received_by_sub2.append(msg.data.decode())
            event_2.set()

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic, data="broadcast message"
            )
            await asyncio.wait_for(event_1.wait(), timeout=self.timeout)
            await asyncio.wait_for(event_2.wait(), timeout=self.timeout)

            assert len(received_by_sub1) == len(received_by_sub2)
            assert "broadcast message" in received_by_sub1
            assert "broadcast message" in received_by_sub2
