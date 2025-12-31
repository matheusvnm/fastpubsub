"""Example: Using Body annotation for explicit field extraction.

This example demonstrates how to use the Body() annotation to
explicitly extract fields from the decoded message body.

Body annotations are useful when:
- You want explicit control over field extraction
- Field names in the message don't match parameter names
- You need to apply type casting
"""

from typing import Annotated

from fastpubsub import Body, FastPubSub, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "body-example",
    topic_name="order-events",
    subscription_name="order-events-sub",
)
async def handle_order(
    # Extract specific fields with different parameter names
    order_id: Annotated[str, Body("orderId")],  # JSON uses camelCase
    total: Annotated[float, Body("totalAmount")],  # By default will cast it.
    items: Annotated[list[dict[str, str | int]], Body("lineItems")],
) -> None:
    """Handle order event with explicit field mapping.

    The JSON message uses camelCase (orderId, totalAmount, lineItems)
    but we can map to snake_case parameter names.
    """
    logger.info(f"Processing order {order_id}")
    logger.info(f"Total: ${total:.2f}")
    logger.info(f"Items: {len(items)} line items")


@broker.subscriber(
    "body-example-2",
    topic_name="order-events",
    subscription_name="order-events-sub-2",
)
async def handle_order_without_annotation(
    # Extract specific fields with different parameter names
    order_id: str = Body("orderId"),  # JSON uses camelCase
    total: float = Body("totalAmount"),  # By default will cast it.
    items: list[dict[str, str | int]] = Body("lineItems"),
) -> None:
    """Handle order event with explicit field mapping but without annotation.

    The JSON message uses camelCase (orderId, totalAmount, lineItems)
    but we can map to snake_case parameter names.
    """
    logger.info(f"Processing order {order_id}")
    logger.info(f"Total: ${total:.2f}")
    logger.info(f"Items: {len(items)} line items")


@app.after_startup
async def test_publish() -> None:
    await broker.publish(
        "order-events",
        data={
            "orderId": "ORD-2024-001",
            "totalAmount": "149.99",  # String that gets cast to float
            "lineItems": [
                {"product": "Widget", "quantity": 2},
                {"product": "Gadget", "quantity": 1},
            ],
            "customerId": "CUST-123",  # Not extracted
        },
    )
