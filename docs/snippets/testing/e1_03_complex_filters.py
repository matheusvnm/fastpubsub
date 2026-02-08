import pytest

from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


@pytest.fixture
def broker() -> PubSubBroker:
    """Create a fresh broker for each test."""
    return PubSubBroker(project_id="test")


@pytest.mark.asyncio
async def test_and_filter_requires_all_conditions(broker: PubSubBroker) -> None:
    """Test that AND filter requires all conditions to match."""
    received: list[Message] = []

    @broker.subscriber(
        alias="priority-orders",
        topic_name="events",
        subscription_name="priority-orders-sub",
        filter_expression='attributes.type = "order" AND attributes.priority = "high"',
    )
    async def handle_priority_orders(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        # High priority order - matches both conditions
        await test_client.publish(
            {"order_id": "001"},
            topic="events",
            attributes={"type": "order", "priority": "high"},
        )

        # Normal priority order - only matches type
        await test_client.publish(
            {"order_id": "002"},
            topic="events",
            attributes={"type": "order", "priority": "normal"},
        )

        # High priority but not an order
        await test_client.publish(
            {"alert_id": "003"},
            topic="events",
            attributes={"type": "alert", "priority": "high"},
        )

    assert len(received) == 1
    assert b"001" in received[0].data


@pytest.mark.asyncio
async def test_or_filter_matches_any_condition(broker: PubSubBroker) -> None:
    """Test that OR filter matches if any condition is true."""
    received: list[Message] = []

    @broker.subscriber(
        alias="financial-events",
        topic_name="events",
        subscription_name="financial-events-sub",
        filter_expression='attributes.type = "order" OR attributes.type = "refund"',
    )
    async def handle_financial(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("order", topic="events", attributes={"type": "order"})
        await test_client.publish("refund", topic="events", attributes={"type": "refund"})
        await test_client.publish("alert", topic="events", attributes={"type": "alert"})

    assert len(received) == 2


@pytest.mark.asyncio
async def test_not_filter_excludes_matching(broker: PubSubBroker) -> None:
    """Test that NOT filter excludes messages that match the condition."""
    received: list[Message] = []

    @broker.subscriber(
        alias="production-events",
        topic_name="events",
        subscription_name="production-events-sub",
        filter_expression='NOT attributes.environment = "test"',
    )
    async def handle_production(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("prod", topic="events", attributes={"environment": "prod"})
        await test_client.publish("test", topic="events", attributes={"environment": "test"})
        await test_client.publish("staging", topic="events", attributes={"environment": "staging"})

    assert len(received) == 2
    for msg in received:
        assert b"test" not in msg.data


@pytest.mark.asyncio
async def test_has_prefix_filter(broker: PubSubBroker) -> None:
    """Test that hasPrefix filter matches attribute prefixes."""
    received: list[Message] = []

    @broker.subscriber(
        alias="us-region-events",
        topic_name="events",
        subscription_name="us-region-events-sub",
        filter_expression='hasPrefix(attributes.region, "us-")',
    )
    async def handle_us_region(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("us-east", topic="events", attributes={"region": "us-east-1"})
        await test_client.publish("us-west", topic="events", attributes={"region": "us-west-2"})
        await test_client.publish("eu", topic="events", attributes={"region": "eu-west-1"})

    assert len(received) == 2


@pytest.mark.asyncio
async def test_existence_filter(broker: PubSubBroker) -> None:
    """Test that existence filter matches when attribute is present."""
    received: list[Message] = []

    @broker.subscriber(
        alias="tagged-events",
        topic_name="events",
        subscription_name="tagged-events-sub",
        filter_expression="attributes:priority",
    )
    async def handle_tagged(msg: Message) -> None:
        received.append(msg)

    async with PubSubTestClient(broker) as test_client:
        await test_client.publish("with-priority", topic="events", attributes={"priority": "high"})
        await test_client.publish("without", topic="events", attributes={"other": "value"})
        await test_client.publish("empty-priority", topic="events", attributes={"priority": ""})

    # Both with-priority and empty-priority have the attribute (even if empty)
    assert len(received) == 2


@pytest.mark.asyncio
async def test_complex_filter_combination(broker: PubSubBroker) -> None:
    """Test complex filter combining multiple operators."""
    priority_orders: list[Message] = []
    financial: list[Message] = []
    production: list[Message] = []
    us_region: list[Message] = []

    @broker.subscriber(
        alias="priority-orders",
        topic_name="events",
        subscription_name="priority-orders-sub",
        filter_expression='attributes.type = "order" AND attributes.priority = "high"',
    )
    async def handle_priority(msg: Message) -> None:
        priority_orders.append(msg)

    @broker.subscriber(
        alias="financial",
        topic_name="events",
        subscription_name="financial-sub",
        filter_expression='attributes.type = "order" OR attributes.type = "refund"',
    )
    async def handle_financial(msg: Message) -> None:
        financial.append(msg)

    @broker.subscriber(
        alias="production",
        topic_name="events",
        subscription_name="production-sub",
        filter_expression='NOT attributes.environment = "test"',
    )
    async def handle_production(msg: Message) -> None:
        production.append(msg)

    @broker.subscriber(
        alias="us-region",
        topic_name="events",
        subscription_name="us-region-sub",
        filter_expression='hasPrefix(attributes.region, "us-")',
    )
    async def handle_us(msg: Message) -> None:
        us_region.append(msg)

    async with PubSubTestClient(broker) as test_client:
        # High priority order from US production
        await test_client.publish(
            {"id": "1"},
            topic="events",
            attributes={
                "type": "order",
                "priority": "high",
                "environment": "prod",
                "region": "us-east-1",
            },
        )

        # Normal order from EU production
        await test_client.publish(
            {"id": "2"},
            topic="events",
            attributes={
                "type": "order",
                "priority": "normal",
                "environment": "prod",
                "region": "eu-west-1",
            },
        )

        # Test environment order
        await test_client.publish(
            {"id": "3"},
            topic="events",
            attributes={
                "type": "order",
                "environment": "test",
                "region": "us-west-2",
            },
        )

        # Refund from EU production
        await test_client.publish(
            {"id": "4"},
            topic="events",
            attributes={
                "type": "refund",
                "environment": "prod",
                "region": "eu-central-1",
            },
        )

    assert len(priority_orders) == 1  # Only high priority order
    assert len(financial) == 4  # All orders and refunds
    assert len(production) == 3  # All except test environment
    assert len(us_region) == 2  # US regions only


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
