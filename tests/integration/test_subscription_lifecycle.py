"""Integration tests for topic and subscription lifecycle management."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from fastpubsub.exceptions import FastPubSubException

if TYPE_CHECKING:
    from collections.abc import Callable

    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestSubscriptionLifecycle:
    """Test topic and subscription CRUD operations."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_autocreate_topic_and_subscription(
        self,
        broker_factory: Callable[..., PubSubBroker],
        project_id: str,
        unique_topic: str,
        unique_subscription: str,
        cleanup_resources: None,
    ) -> None:
        """Test automatic topic and subscription creation."""
        from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

        broker = broker_factory()

        @broker.subscriber(
            alias="test-autocreate",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: str) -> None:
            pass

        # Start broker (should create resources)
        await broker.start()
        try:
            await asyncio.sleep(1.0)  # Give time for resources to be created

            # Verify topic exists
            pub_client = PublisherClient()
            topic_path = pub_client.topic_path(project_id, unique_topic)
            topic_obj = pub_client.get_topic(request={"topic": topic_path})
            assert topic_obj.name == topic_path

            # Verify subscription exists
            sub_client = SubscriberClient()
            sub_path = sub_client.subscription_path(project_id, unique_subscription)
            sub_obj = sub_client.get_subscription(request={"subscription": sub_path})
            assert sub_obj.name == sub_path
        finally:
            broker.shutdown()

    @pytest.mark.asyncio
    async def test_autocreate_false_requires_existing_resources(
        self,
        broker_factory: Callable[..., PubSubBroker],
        unique_topic: str,
        unique_subscription: str,
    ) -> None:
        """Test that autocreate=False fails when resources don't exist."""
        from google.api_core.exceptions import NotFound

        broker = broker_factory()

        @broker.subscriber(
            alias="test-no-autocreate",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=False,
        )
        async def handler(msg: str) -> None:
            pass

        # Should fail because topic/subscription don't exist
        with pytest.raises((FastPubSubException, NotFound)):
            await broker.start()

    @pytest.mark.asyncio
    async def test_broker_health_checks(
        self,
        broker_factory: Callable[..., PubSubBroker],
        unique_topic: str,
        unique_subscription: str,
        cleanup_resources: None,
    ) -> None:
        """Test broker alive and ready health checks."""
        broker = broker_factory()

        @broker.subscriber(
            alias="test-health",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: str) -> None:
            pass

        # Before start, health checks should fail
        assert broker.alive() is False
        assert broker.ready() is False

        await broker.start()
        await asyncio.sleep(0.5)

        try:
            # After start, health checks should pass
            assert broker.alive() is True
            assert broker.ready() is True
        finally:
            broker.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_subscribers_same_topic(
        self,
        broker_factory: Callable[..., PubSubBroker],
        unique_topic: str,
        cleanup_resources: None,
    ) -> None:
        """Test multiple subscribers on the same topic."""
        broker = broker_factory()
        received_by_sub1: list[str] = []
        received_by_sub2: list[str] = []
        event = asyncio.Event()

        @broker.subscriber(
            alias="subscriber-1",
            topic_name=unique_topic,
            subscription_name=f"{unique_topic}-sub-1",
            autocreate=True,
        )
        async def handler1(msg: str) -> None:
            received_by_sub1.append(msg)
            if len(received_by_sub1) >= 1 and len(received_by_sub2) >= 1:
                event.set()

        @broker.subscriber(
            alias="subscriber-2",
            topic_name=unique_topic,
            subscription_name=f"{unique_topic}-sub-2",
            autocreate=True,
        )
        async def handler2(msg: str) -> None:
            received_by_sub2.append(msg)
            if len(received_by_sub1) >= 1 and len(received_by_sub2) >= 1:
                event.set()

        await broker.start()
        try:
            await asyncio.sleep(0.5)

            await broker.publish(topic_name=unique_topic, data="broadcast message")

            await asyncio.wait_for(event.wait(), timeout=15.0)

            # Both subscribers should receive the message
            assert len(received_by_sub1) >= 1
            assert len(received_by_sub2) >= 1
            assert "broadcast message" in received_by_sub1
            assert "broadcast message" in received_by_sub2
        finally:
            broker.shutdown()
