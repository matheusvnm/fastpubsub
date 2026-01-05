"""Example: Working with Pydantic models.

This example demonstrates how Pydantic models are automatically
serialized when publishing and can be reconstructed when receiving.

Pydantic models provide:
- Automatic JSON serialization
- Type validation
- Rich data modeling capabilities
"""

from datetime import datetime
from typing import Annotated
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from fastpubsub import FastPubSub, Header, PubSubBroker
from fastpubsub.logger import logger


# Define Pydantic models for your events
class Address(BaseModel):
    """User address model."""

    street: str
    city: str
    country: str
    postal_code: str


class UserCreatedEvent(BaseModel):
    """Event emitted when a user is created."""

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.now)
    user_id: str
    email: str
    name: str
    address: Address | None = None


class OrderPlacedEvent(BaseModel):
    """Event emitted when an order is placed."""

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.now)
    order_id: str
    user_id: str
    total_amount: float
    items: list[dict[str, str | int | float]]


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "user-created-handler",
    topic_name="user-events",
    subscription_name="user-events-sub",
)
async def handle_user_created(
    trace_id: Annotated[str, Header("x-trace-id", default="no-trace")],
    # Receive the entire decoded body as a dict, then reconstruct
    user_id: str,
    email: str,
    name: str,
) -> None:
    """Handle user created event.

    The JSON is auto-unwrapped, extracting user_id, email, and name.
    """
    logger.info(f"[{trace_id}] New user created: {name} ({email})")
    logger.info(f"User ID: {user_id}")


@broker.subscriber(
    "order-placed-handler",
    topic_name="order-events",
    subscription_name="order-events-sub",
)
async def handle_order_placed(event: OrderPlacedEvent) -> None:
    """Handle order placed event.

    Receives it as a Pydantic model.
    """
    logger.info(f"Order {event.order_id} placed by user {event.user_id}")
    logger.info(f"Total: ${event.total_amount:.2f}, Items: {len(event.items)}")


@app.after_startup
async def test_publish() -> None:
    # Publish Pydantic models - they are automatically serialized to JSON
    user_event = UserCreatedEvent(
        user_id="user-123",
        email="john@example.com",
        name="John Doe",
        address=Address(
            street="123 Main St",
            city="New York",
            country="USA",
            postal_code="10001",
        ),
    )
    await broker.publish(
        "user-events",
        user_event,
        attributes={"x-trace-id": "trace-user-001"},
    )

    order_event = OrderPlacedEvent(
        order_id="ORD-2024-001",
        user_id="user-123",
        total_amount=149.99,
        items=[
            {"product": "Widget", "quantity": 2, "price": 49.99},
            {"product": "Gadget", "quantity": 1, "price": 50.01},
        ],
    )
    await broker.publish("order-events", order_event)
