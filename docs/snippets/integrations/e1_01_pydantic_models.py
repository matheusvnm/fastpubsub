"""Title: Pydantic Model Integration

Demonstrates how to use Pydantic models with FastPubSub for message validation.

This example shows:
- Publishing Pydantic models (automatic JSON serialization)
- Validating incoming messages with Pydantic
- Handling validation errors gracefully
- Using schema evolution with optional fields

Pydantic provides type-safe message handling and automatic validation,
catching data issues early and making your handlers more robust.

Run with:
    fastpubsub run examples.integrations.e1_01_pydantic_models:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:pydantic_integration_full]
from pydantic import BaseModel, Field, ValidationError

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Drop
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-local")
app = FastPubSub(broker)


# --8<-- [start:pydantic_schemas]
# Define message schemas using Pydantic
class OrderEvent(BaseModel):
    """Schema for order events."""

    order_id: str = Field(min_length=1, description="Unique order identifier")
    customer_id: str = Field(min_length=1, description="Customer identifier")
    total: float = Field(gt=0, description="Order total amount")
    items: list[str] = Field(default_factory=list, description="List of item names")
    priority: str = Field(default="normal", description="Order priority level")


class UserEvent(BaseModel):
    """Schema for user events with optional fields for backward compatibility."""

    user_id: str
    email: str = Field(pattern=r"^[\w.-]+@[\w.-]+\.\w+$")
    action: str
    # Optional field for schema evolution
    metadata: dict | None = None


# --8<-- [end:pydantic_schemas]


# Storage for received events
received_orders: list[OrderEvent] = []
received_users: list[UserEvent] = []


# --8<-- [start:pydantic_handler]
@broker.subscriber(
    alias="order-handler",
    topic_name="order-events",
    subscription_name="order-events-subscription",
    autocreate=True,
)
async def handle_order(message: Message) -> None:
    """Handle order events with Pydantic validation."""
    try:
        # Parse and validate the message data
        order = OrderEvent.model_validate_json(message.data)

        logger.info(
            f"Received valid order: {order.order_id}",
            extra={
                "order_id": order.order_id,
                "customer_id": order.customer_id,
                "total": order.total,
            },
        )

        received_orders.append(order)

    except ValidationError as e:
        # Log validation errors and drop invalid messages
        logger.warning(f"Invalid order event: {e}")
        raise Drop(f"Validation failed: {e}")


# --8<-- [end:pydantic_handler]


@broker.subscriber(
    alias="user-handler",
    topic_name="user-events",
    subscription_name="user-events-subscription",
    autocreate=True,
)
async def handle_user(message: Message) -> None:
    """Handle user events with Pydantic validation."""
    try:
        user = UserEvent.model_validate_json(message.data)

        logger.info(
            f"Received user event: {user.action} for {user.user_id}",
            extra={"user_id": user.user_id, "action": user.action},
        )

        received_users.append(user)

    except ValidationError as e:
        logger.warning(f"Invalid user event: {e}")
        raise Drop(f"Validation failed: {e}")


@app.after_startup
async def test_pydantic_integration() -> None:
    """Publish test messages to demonstrate Pydantic integration."""
    # Publish a valid order using Pydantic model
    order = OrderEvent(
        order_id="ORD-001",
        customer_id="CUST-123",
        total=99.99,
        items=["Widget", "Gadget"],
        priority="high",
    )
    await broker.publish("order-events", order)
    logger.info("Published OrderEvent model")

    # Publish a valid user event using Pydantic model
    user = UserEvent(
        user_id="USR-456",
        email="user@example.com",
        action="login",
    )
    await broker.publish("user-events", user)
    logger.info("Published UserEvent model")

    # Publish using dict (also works)
    await broker.publish(
        "order-events",
        {
            "order_id": "ORD-002",
            "customer_id": "CUST-789",
            "total": 149.99,
            "items": ["Premium Widget"],
        },
    )
    logger.info("Published dict (will be validated as OrderEvent)")

    # Publish invalid data to demonstrate validation
    await broker.publish(
        "user-events",
        {"user_id": "USR-000", "email": "invalid-email", "action": "test"},
    )
    logger.info("Published invalid email (will be dropped)")


# --8<-- [end:pydantic_integration_full]
