"""Example: Using PubSubTestClient for test assertions.

This example demonstrates how to use the test client in actual pytest
tests with assertions on message routing and content.

Run with: pytest examples/testing/e2_01_test_assertions.py -v
"""

import pytest

from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


@pytest.fixture
def broker() -> PubSubBroker:
    return PubSubBroker(project_id="test")


@pytest.mark.asyncio
async def test_order_processing(broker: PubSubBroker) -> None:
    """Test that orders are processed correctly."""
    processed_orders: list[Message] = []

    @broker.subscriber(
        alias="order-processor",
        topic_name="orders",
        subscription_name="order-processor-sub",
    )
    async def process_order(msg: Message) -> None:
        processed_orders.append(msg)

    async with PubSubTestClient(broker) as client:
        await client.publish({"order_id": "1", "amount": 100}, topic="orders")
        await client.publish({"order_id": "2", "amount": 200}, topic="orders")

    assert len(processed_orders) == 2


@pytest.mark.asyncio
async def test_filtered_events(broker: PubSubBroker) -> None:
    """Test that filter expressions work correctly."""
    vip_orders: list[Message] = []
    regular_orders: list[Message] = []

    @broker.subscriber(
        alias="vip-handler",
        topic_name="orders",
        subscription_name="vip-orders-sub",
        filter_expression='attributes.customer_tier = "vip"',
    )
    async def handle_vip(msg: Message) -> None:
        vip_orders.append(msg)

    @broker.subscriber(
        alias="regular-handler",
        topic_name="orders",
        subscription_name="regular-orders-sub",
        filter_expression='attributes.customer_tier = "regular"',
    )
    async def handle_regular(msg: Message) -> None:
        regular_orders.append(msg)

    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"order": "vip-1"},
            topic="orders",
            attributes={"customer_tier": "vip"},
        )
        await client.publish(
            {"order": "regular-1"},
            topic="orders",
            attributes={"customer_tier": "regular"},
        )
        await client.publish(
            {"order": "vip-2"},
            topic="orders",
            attributes={"customer_tier": "vip"},
        )

    assert len(vip_orders) == 2
    assert len(regular_orders) == 1


@pytest.mark.asyncio
async def test_message_inspection(broker: PubSubBroker) -> None:
    """Test that we can inspect published messages."""

    @broker.subscriber(
        alias="dummy",
        topic_name="audit",
        subscription_name="audit-sub",
    )
    async def handler(msg: Message) -> None:
        pass

    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"action": "create"},
            topic="audit",
            attributes={"user": "alice"},
        )
        await client.publish(
            {"action": "delete"},
            topic="audit",
            attributes={"user": "bob"},
        )

        messages = client.get_published_messages()

        assert len(messages) == 2
        assert messages[0][0] == "audit"
        assert messages[1][2] == {"user": "bob"}


@pytest.mark.asyncio
async def test_complex_filter_routing(broker: PubSubBroker) -> None:
    """Test complex filter expression routing."""
    urgent_us: list[Message] = []
    all_urgent: list[Message] = []

    @broker.subscriber(
        alias="urgent-us",
        topic_name="alerts",
        subscription_name="urgent-us-sub",
        filter_expression='attributes.severity = "urgent" AND hasPrefix(attributes.region, "us-")',
    )
    async def handle_urgent_us(msg: Message) -> None:
        urgent_us.append(msg)

    @broker.subscriber(
        alias="all-urgent",
        topic_name="alerts",
        subscription_name="all-urgent-sub",
        filter_expression='attributes.severity = "urgent"',
    )
    async def handle_all_urgent(msg: Message) -> None:
        all_urgent.append(msg)

    async with PubSubTestClient(broker) as client:
        await client.publish(
            {"alert": "disk full"},
            topic="alerts",
            attributes={"severity": "urgent", "region": "us-east-1"},
        )
        await client.publish(
            {"alert": "high cpu"},
            topic="alerts",
            attributes={"severity": "urgent", "region": "eu-west-1"},
        )
        await client.publish(
            {"alert": "low disk"},
            topic="alerts",
            attributes={"severity": "warning", "region": "us-west-2"},
        )

    assert len(urgent_us) == 1
    assert len(all_urgent) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
