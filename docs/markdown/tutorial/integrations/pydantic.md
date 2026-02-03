---
icon: lucide/check-circle
---

# Pydantic Integration

FastPubSub integrates with [Pydantic](https://docs.pydantic.dev/) for data validation and serialization. This integration enables type-safe message handling and automatic JSON conversion.

## How FastPubSub Uses Pydantic

FastPubSub leverages Pydantic in three ways:

1. **Message Serialization** - Pydantic models are automatically converted to JSON when publishing
2. **Push Message Handling** - Built-in models for HTTP push endpoints
3. **Parameter Validation** - Public APIs use `@validate_call` for type checking

## Publishing Pydantic Models

When you publish a Pydantic model, FastPubSub automatically serializes it to JSON:

```python
from pydantic import BaseModel
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

class OrderEvent(BaseModel):
    order_id: str
    customer_id: str
    total: float
    items: list[str]

@app.post("/create-order")
async def create_order(order: OrderEvent):
    # Pydantic model is automatically serialized to JSON
    await broker.publish("orders", order)  # (1)!
    return {"status": "created"}
```

1. FastPubSub calls `order.model_dump_json()` internally

### Supported Data Types

FastPubSub accepts multiple data types for publishing:

| Type | Serialization |
|------|---------------|
| `bytes` | Sent as-is |
| `str` | UTF-8 encoded |
| `dict` | JSON serialized |
| `BaseModel` | JSON serialized via `model_dump_json()` |

```python
# All of these work:
await broker.publish("topic", b"raw bytes")
await broker.publish("topic", "string message")
await broker.publish("topic", {"key": "value"})
await broker.publish("topic", OrderEvent(order_id="123", ...))
```

## Validating Incoming Messages

Parse and validate incoming message data using Pydantic models in your handlers:

```python
from pydantic import BaseModel, ValidationError
from fastpubsub import Message
from fastpubsub.exceptions import Drop

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
        event = UserEvent.model_validate_json(message.data)  # (1)!

        await process_user_event(event)

    except ValidationError as e:
        # Invalid data - drop the message
        raise Drop(f"Invalid user event: {e}")
```

1. Use `model_validate_json()` to parse bytes directly

### Validation Patterns

=== "Required Fields"
    ```python
    from pydantic import BaseModel

    class PaymentEvent(BaseModel):
        payment_id: str
        amount: float  # Required field

    @broker.subscriber(...)
    async def handle_payment(message: Message):
        # Raises ValidationError if amount is missing
        event = PaymentEvent.model_validate_json(message.data)
    ```

=== "Optional Fields"
    ```python
    from pydantic import BaseModel

    class NotificationEvent(BaseModel):
        user_id: str
        title: str
        body: str | None = None  # Optional field

    @broker.subscriber(...)
    async def handle_notification(message: Message):
        event = NotificationEvent.model_validate_json(message.data)
        # body will be None if not provided
    ```

=== "Field Constraints"
    ```python
    from pydantic import BaseModel, Field

    class OrderEvent(BaseModel):
        order_id: str = Field(min_length=1)
        quantity: int = Field(gt=0, le=1000)
        email: str = Field(pattern=r"^[\w.-]+@[\w.-]+\.\w+$")

    @broker.subscriber(...)
    async def handle_order(message: Message):
        # Validates constraints automatically
        event = OrderEvent.model_validate_json(message.data)
    ```

## Push Message Models

FastPubSub provides built-in Pydantic models for handling HTTP push subscriptions:

```python
from fastpubsub import FastPubSub, PubSubBroker, PushMessage
import base64

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@app.post("/push-endpoint")
async def receive_push(push_message: PushMessage):  # (1)!
    # Access the nested message content
    message_id = push_message.message.id
    subscription = push_message.subscription

    # Decode base64 data
    raw_data = base64.b64decode(push_message.message.data)

    # Parse as your domain model
    event = OrderEvent.model_validate_json(raw_data)

    await process_event(event)
    return {"status": "ok"}
```

1. FastAPI automatically validates the incoming JSON against `PushMessage`

### PushMessage Structure

```python
class PushMessageContent(BaseModel):
    id: str              # Message ID (alias: messageId)
    data: str            # Base64-encoded message data
    publish_time: str    # Publish timestamp (alias: publishTime)
    attributes: dict[str, str] = {}

class PushMessage(BaseModel):
    subscription: str    # Full subscription path
    message: PushMessageContent
```

!!! info "Base64 Encoding"
    Push messages from Pub/Sub have their data base64-encoded. Use `base64.b64decode()` to get the raw bytes before parsing.

## Schema Evolution

Handle message schema changes gracefully:

### Adding New Fields

```python
from pydantic import BaseModel

class OrderEventV2(BaseModel):
    order_id: str
    customer_id: str
    total: float
    # New field with default - backward compatible
    priority: str = "normal"

@broker.subscriber(...)
async def handle_order(message: Message):
    # Works with both old (no priority) and new messages
    event = OrderEventV2.model_validate_json(message.data)
```

### Handling Unknown Fields

```python
from pydantic import BaseModel, ConfigDict

class FlexibleEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")  # (1)!

    order_id: str
    # Unknown fields are silently ignored

class StrictEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")  # (2)!

    order_id: str
    # Unknown fields raise ValidationError
```

1. Ignore extra fields from newer message versions
2. Fail if message has unexpected fields

## Best Practices

!!! tip "Use Explicit Models"
    Define Pydantic models for all message types. This documents your message schema and catches errors early.

!!! tip "Handle Validation Errors"
    Always wrap `model_validate_json()` in try/except. Invalid messages should be dropped, not retried forever.

!!! tip "Version Your Schemas"
    Use optional fields with defaults when evolving schemas. This maintains backward compatibility during deployments.

!!! tip "Validate at Boundaries"
    Validate incoming messages at the handler entry point. Trust your own Pydantic models when publishing.

## Recap

- FastPubSub **automatically serializes** Pydantic models to JSON when publishing
- Use `model_validate_json()` to **parse and validate** incoming messages
- Handle `ValidationError` by **raising Drop** for invalid messages
- Built-in `PushMessage` model handles **HTTP push endpoints**
- Use **optional fields with defaults** for backward-compatible schema evolution
- **Next**: Learn about [Uvicorn Integration](uvicorn.md) for production deployments
