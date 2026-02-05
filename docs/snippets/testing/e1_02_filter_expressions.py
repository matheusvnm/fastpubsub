import pytest

from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


@pytest.fixture
def broker() -> PubSubBroker:
    """Create a fresh broker for each test."""
    return PubSubBroker(project_id="test")


# --8<-- [start:filter_routing_test]
@pytest.mark.asyncio
async def test_filter_routes_to_correct_handler(broker: PubSubBroker) -> None:
    """Test that filter expressions route messages to the correct handlers."""
    order_messages: list[Message] = []
    user_messages: list[Message] = []
    all_messages: list[Message] = []

    @broker.subscriber(
        alias="order-handler",
        topic_name="events",
        subscription_name="order-events-sub",
        filter_expression='attributes.event_type = "order"',
    )
    async def handle_orders(msg: Message) -> None:
        order_messages.append(msg)

    @broker.subscriber(
        alias="user-handler",
        topic_name="events",
        subscription_name="user-events-sub",
        filter_expression='attributes.event_type = "user"',
    )
    async def handle_users(msg: Message) -> None:
        user_messages.append(msg)

    @broker.subscriber(
        alias="all-events-handler",
        topic_name="events",
        subscription_name="all-events-sub",
    )
    async def handle_all(msg: Message) -> None:
        all_messages.append(msg)

    async with PubSubTestClient(broker) as test_client:
        # Order event - should go to order-handler and all-events-handler
        await test_client.publish(
            {"order_id": "12345", "amount": 99.99},
            topic="events",
            attributes={"event_type": "order"},
        )

        # User event - should go to user-handler and all-events-handler
        await test_client.publish(
            {"user_id": "abc", "action": "login"},
            topic="events",
            attributes={"event_type": "user"},
        )

        # Unknown event - should only go to all-events-handler
        await test_client.publish(
            {"data": "unknown"},
            topic="events",
            attributes={"event_type": "unknown"},
        )

    # Verify routing
    assert len(order_messages) == 1
    assert len(user_messages) == 1
    assert len(all_messages) == 3  # Receives all events (no filter)


# --8<-- [end:filter_routing_test]


@pytest.mark.asyncio
async def test_filter_with_no_matching_messages(broker: PubSubBroker) -> None:
    """Test that a subscriber with a filter receives nothing if no messages match."""
    received: list[Message] = []

    @broker.subscriber(
        alias="vip-handler",
        topic_name="events",
        subscription_name="vip-sub",
        filter_expression='attributes.customer_type = "vip"',
    )
    async def handle_vip(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        # Publish messages that don't match the filter
        await test_client.publish("msg1", topic="events", attributes={"customer_type": "regular"})
        await test_client.publish("msg2", topic="events", attributes={"customer_type": "basic"})

    assert len(received) == 0


@pytest.mark.asyncio
async def test_filter_with_missing_attributes(broker: PubSubBroker) -> None:
    """Test that messages without required attributes are filtered out."""
    received: list[Message] = []

    @broker.subscriber(
        alias="typed-handler",
        topic_name="events",
        subscription_name="typed-sub",
        filter_expression='attributes.type = "important"',
    )
    async def handle_typed(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        # Message without attributes - should not match
        await test_client.publish("no-attrs", topic="events")

        # Message with wrong attribute - should not match
        await test_client.publish("wrong-attr", topic="events", attributes={"other": "value"})

        # Message with correct attribute - should match
        await test_client.publish("correct", topic="events", attributes={"type": "important"})

    assert len(received) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
