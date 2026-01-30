"""Title: Basic PubSubTestClient Usage

Demonstrates the basic usage of PubSubTestClient for testing subscriber handlers.

This example shows:
- Creating a test broker fixture for pytest
- Using PubSubTestClient as an async context manager
- Publishing messages and verifying subscriber receives them
- Publishing dictionary data (JSON serialized automatically)
- Inspecting all published messages with get_published_messages()

PubSubTestClient enables fast, isolated unit tests without needing a real
Google Cloud Pub/Sub emulator.

Run with:
    pytest examples/testing/e1_01_test_client.py -v
"""

import pytest

from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


@pytest.fixture
def broker() -> PubSubBroker:
    """Create a fresh broker for each test."""
    return PubSubBroker(project_id="test")


@pytest.mark.asyncio
async def test_basic_publish_and_subscribe(broker: PubSubBroker) -> None:
    """Test that a message is received by a subscriber."""
    received_messages: list[Message] = []

    @broker.subscriber(
        alias="test",
        topic_name="test-topic",
        subscription_name="test-sub",
    )
    async def handler(msg: Message) -> None:
        received_messages.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("Hello", topic="test-topic")

    assert len(received_messages) == 1
    assert received_messages[0].data == b"Hello"


@pytest.mark.asyncio
async def test_publish_dict_data(broker: PubSubBroker) -> None:
    """Test publishing dictionary data that gets JSON serialized."""
    received_messages: list[Message] = []

    @broker.subscriber(
        alias="dict-handler",
        topic_name="test-topic",
        subscription_name="dict-sub",
    )
    async def handler(msg: Message) -> None:
        received_messages.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish({"key": "value", "number": 42}, topic="test-topic")

    assert len(received_messages) == 1
    assert b'"key"' in received_messages[0].data
    assert b'"value"' in received_messages[0].data


@pytest.mark.asyncio
async def test_inspect_published_messages(broker: PubSubBroker) -> None:
    """Test that we can inspect all published messages."""

    @broker.subscriber(
        alias="inspector",
        topic_name="test-topic",
        subscription_name="inspector-sub",
    )
    async def handler(msg: Message) -> None:
        pass

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("msg1", topic="test-topic")
        await test_client.publish("msg2", topic="test-topic")
        await test_client.publish("msg3", topic="other-topic")

        messages = test_client.get_published_messages()

    assert len(messages) == 3
    assert messages[0][0] == "test-topic"
    assert messages[2][0] == "other-topic"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
