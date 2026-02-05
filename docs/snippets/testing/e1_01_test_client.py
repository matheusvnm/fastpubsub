# --8<-- [start:test_client_full]
import pytest

from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


# --8<-- [start:broker_fixture]
@pytest.fixture
def broker() -> PubSubBroker:
    """Create a fresh broker for each test."""
    return PubSubBroker(project_id="test")


# --8<-- [end:broker_fixture]


# --8<-- [start:basic_test]
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


# --8<-- [end:basic_test]


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
# --8<-- [end:test_client_full]
