import pytest

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient

# --8<-- [start:app]
broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)


@broker.subscriber(
    alias="order-handler",
    topic_name="incoming-orders",
    subscription_name="incoming-orders-sub",
)
async def handle_order(message: Message) -> None:
    order_id = message.data.decode("utf-8")
    await broker.publish(
        topic_name="order-confirmations",
        data=f"confirmed-{order_id}",
    )


# --8<-- [end:app]


# --8<-- [start:test]
@pytest.mark.asyncio
async def test_message_is_forwarded():
    async with PubSubTestClient(broker) as client:
        await client.publish("order-123", topic="incoming-orders")

        messages = client.get_published_messages()

    # The first published message is our test publish to "incoming-orders".
    # The second is the one the handler forwarded to "order-confirmations".
    assert len(messages) == 2
    assert messages[1].topic_name == "order-confirmations"
    assert messages[1].data == b"confirmed-order-123"


# --8<-- [end:test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
