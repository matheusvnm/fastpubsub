import json

import pytest

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient

# --8<-- [start:app]
broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)


@broker.subscriber(
    alias="order-processor",
    topic_name="orders",
    subscription_name="orders-sub",
)
async def process_order(message: Message) -> str:
    order = json.loads(message.data)
    if order["amount"] <= 0:
        raise ValueError("Invalid order amount")
    return f"processed-{order['id']}"


# --8<-- [end:app]


# --8<-- [start:test]
@pytest.mark.asyncio
async def test_order_is_processed():
    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"id": "order-1", "amount": 100},
            topic="orders",
        )

        results = client.get_results()

    assert len(results) == 1
    assert results[0].return_value == "processed-order-1"
    assert results[0].error is None
    assert results[0].message.topic_name == "orders"


@pytest.mark.asyncio
async def test_invalid_order_raises_error():
    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"id": "order-2", "amount": -5},
            topic="orders",
        )

        results = client.get_results()

    assert len(results) == 1
    assert results[0].return_value is None
    assert isinstance(results[0].error, ValueError)


# --8<-- [end:test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
