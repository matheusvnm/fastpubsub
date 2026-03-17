"""Integration test fixtures for FastPubSub."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from fastpubsub.broker import PubSubBroker


@pytest.fixture
async def cleanup_resources(
    project_id: str,
    unique_topic: str,
    unique_subscription: str,
) -> AsyncGenerator[None, None]:
    """Clean up PubSub resources after test.

    This fixture yields control to the test, then cleans up
    any created topics and subscriptions.
    """
    yield

    # Import here to avoid issues when emulator is not available
    from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

    # Cleanup topic
    try:
        pub_client = PublisherClient()
        topic_path = pub_client.topic_path(project_id, unique_topic)
        pub_client.delete_topic(request={"topic": topic_path})
    except Exception:
        pass  # Ignore cleanup errors

    # Cleanup subscription
    try:
        sub_client = SubscriberClient()
        sub_path = sub_client.subscription_path(
            project_id, unique_subscription
        )
        sub_client.delete_subscription(request={"subscription": sub_path})
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
async def connected_broker(
    project_id: str,
    unique_topic: str,
    unique_subscription: str,
    broker_factory: Callable[..., PubSubBroker],
) -> AsyncGenerator[PubSubBroker, None]:
    """Provide a broker connected to the emulator.

    This fixture creates a broker, yields it for the test,
    then shuts it down and cleans up resources.
    """

    broker = broker_factory()
    yield broker

    from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient

    try:
        pub_client = PublisherClient()
        topic_path = pub_client.topic_path(project_id, unique_topic)
        pub_client.delete_topic(request={"topic": topic_path})
    except Exception:
        pass  # Ignore cleanup errors

    # Cleanup subscription
    try:
        sub_client = SubscriberClient()
        sub_path = sub_client.subscription_path(
            project_id, unique_subscription
        )
        sub_client.delete_subscription(request={"subscription": sub_path})
    except Exception:
        pass  # Ignore cleanup errors


@asynccontextmanager
async def managed_broker(broker: PubSubBroker) -> AsyncGenerator[None, None]:
    await broker.start()
    await asyncio.sleep(1)
    try:
        yield
    finally:
        await broker.shutdown()
