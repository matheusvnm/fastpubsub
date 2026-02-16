import pytest

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient

# --8<-- [start:app]
broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)


@broker.subscriber(
    alias="order-handler",
    topic_name="events",
    subscription_name="order-sub",
    filter_expression='attributes.event_type = "order"',
)
async def handle_orders(message: Message) -> str:
    return message.data.decode("utf-8")


# --8<-- [end:app]


# --8<-- [start:test]
@pytest.mark.asyncio
async def test_filter_expression_routes_expected_messages() -> None:
    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"order_id": "ord-1"},
            topic="events",
            attributes={"event_type": "order"},
        )
        await client.publish(
            {"user_id": "usr-1"},
            topic="events",
            attributes={"event_type": "user"},
        )

        results = client.get_results()

    assert len(results) == 1
    assert results[0].message.attributes["event_type"] == "order"


# --8<-- [end:test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
