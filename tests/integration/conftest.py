"""Integration test fixtures for FastPubSub."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

import pytest
from fastpubsub_core import AdminClient, CoreError

from fastpubsub._internal.connection import Connection

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable

    from fastpubsub.broker import PubSubBroker


def delete_resources(
    project_id: str, topic_name: str, subscription_name: str
) -> None:
    """Remove a test topic and a test subscription, if they exist."""
    connection = Connection(project_id)
    admin_client = AdminClient(Connection.read_endpoint())
    with suppress(CoreError):
        admin_client.delete_topic(connection.topic_path(topic_name))
    with suppress(CoreError):
        admin_client.delete_subscription(
            connection.subscription_path(subscription_name)
        )


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
    await asyncio.to_thread(
        delete_resources, project_id, unique_topic, unique_subscription
    )


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
    await asyncio.to_thread(
        delete_resources, project_id, unique_topic, unique_subscription
    )


@asynccontextmanager
async def managed_broker(broker: PubSubBroker) -> AsyncGenerator[None, None]:
    await broker.start()
    await asyncio.sleep(1)
    try:
        yield
    finally:
        await broker.shutdown()
