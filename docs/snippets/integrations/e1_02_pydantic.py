import base64

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from fastpubsub import FastPubSub, Message, PubSubBroker, PushMessage
from fastpubsub.logger import logger
from fastpubsub.exceptions import Drop

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:publish_model]
class OrderEvent(BaseModel):
    order_id: str
    customer_id: str
    total: float
    items: list[str]


@app.post("/create-order")
async def create_order(order: OrderEvent):
    # Pydantic model is automatically serialized to JSON
    await broker.publish("orders", order)
    return {"status": "created"}


# --8<-- [end:publish_model]


async def process_user_event(event: "UserEvent") -> None:
    """Process user event."""
    pass


# --8<-- [start:validate_incoming]
class UserEvent(BaseModel):
    user_id: str
    email: str
    action: str


@broker.subscriber(
    alias="user-handler",
    topic_name="user-events",
    subscription_name="user-events-subscription",
)
async def handle_user_event(message: Message):
    try:
        # Parse and validate the message data
        event = UserEvent.model_validate_json(message.data)

        await process_user_event(event)

    except ValidationError as e:
        # Invalid data - drop the message
        raise Drop(f"Invalid user event: {e}")


# --8<-- [end:validate_incoming]


# --8<-- [start:required_fields]
class PaymentEvent(BaseModel):
    payment_id: str
    amount: float  # Required field


@broker.subscriber(
    alias="payment-handler",
    topic_name="payments",
    subscription_name="payments-subscription",
)
async def handle_payment(message: Message):
    # Raises ValidationError if amount is missing
    event = PaymentEvent.model_validate_json(message.data)


# --8<-- [end:required_fields]


# --8<-- [start:optional_fields]
class NotificationEvent(BaseModel):
    user_id: str
    title: str
    body: str | None = None  # Optional field


@broker.subscriber(
    alias="notification-handler",
    topic_name="notifications",
    subscription_name="notifications-subscription",
)
async def handle_notification(message: Message):
    event = NotificationEvent.model_validate_json(message.data)
    # body will be None if not provided


# --8<-- [end:optional_fields]


# --8<-- [start:field_constraints]
class ConstrainedOrderEvent(BaseModel):
    order_id: str = Field(min_length=1)
    quantity: int = Field(gt=0, le=1000)
    email: str = Field(pattern=r"^[\w.-]+@[\w.-]+\.\w+$")


@broker.subscriber(
    alias="constrained-order-handler",
    topic_name="constrained-orders",
    subscription_name="constrained-orders-subscription",
)
async def handle_constrained_order(message: Message):
    # Validates constraints automatically
    event = ConstrainedOrderEvent.model_validate_json(message.data)


# --8<-- [end:field_constraints]


async def process_event(event: OrderEvent) -> None:
    """Process event."""
    pass


# --8<-- [start:push_endpoint]
@app.post("/push-endpoint")
async def receive_push(push_message: PushMessage):
    # Access the nested message content
    message_id = push_message.message.id
    subscription = push_message.subscription

    logger.info(f"We received {message_id=} for {subscription=}")

    # Decode base64 data
    raw_data = base64.b64decode(push_message.message.data)

    # Parse as your domain model
    event = OrderEvent.model_validate_json(raw_data)

    await process_event(event)
    return {"status": "ok"}


# --8<-- [end:push_endpoint]


# --8<-- [start:schema_evolution]
class OrderEventV2(BaseModel):
    order_id: str
    customer_id: str
    total: float
    # New field with default - backward compatible
    priority: str = "normal"


@broker.subscriber(
    alias="order-v2-handler",
    topic_name="orders-v2",
    subscription_name="orders-v2-subscription",
)
async def handle_order_v2(message: Message):
    # Works with both old (no priority) and new messages
    event = OrderEventV2.model_validate_json(message.data)


# --8<-- [end:schema_evolution]


# --8<-- [start:extra_handling]
class FlexibleEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    order_id: str
    # Unknown fields are silently ignored


class StrictEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str
    # Unknown fields raise ValidationError


# --8<-- [end:extra_handling]
