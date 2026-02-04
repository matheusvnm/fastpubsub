---
icon: lucide/check-circle
---

# Pydantic Integration

FastPubSub integrates with [Pydantic](https://docs.pydantic.dev/) for data validation and serialization. This integration enables type-safe message handling and automatic JSON conversion.

## How FastPubSub Uses Pydantic

FastPubSub leverages Pydantic in three ways:

1. **Message Serialization** - Pydantic models are automatically converted to JSON when publishing.
2. **Push Message Handling** - Built-in models for HTTP push endpoints.
3. **Parameter Validation** - Public APIs use `@validate_call` for type checking.

---

## Step-by-Step

1. Define a Pydantic model for your message schema.
2. Publish model instances with `broker.publish(...)`.
3. Parse incoming bytes with `model_validate_json()` in handlers.
4. Handle validation errors by raising `Drop`.

## Publishing Pydantic Models

When you publish a Pydantic model, FastPubSub automatically serializes it to JSON:

```python
--8<-- "integrations/e1_02_pydantic.py:publish_model"
```

1. FastPubSub calls `order.model_dump_json()` internally

!!! note "Pydantic Version"
    The examples use Pydantic v2 (`model_dump_json`, `model_validate_json`). We will not provide Pydantic v1 as it is already deprecated.

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
--8<-- "integrations/e1_02_pydantic.py:validate_incoming"
```

1. Use `model_validate_json()` to parse bytes directly

### Validation Patterns

=== "Required Fields"
    ```python
    --8<-- "integrations/e1_02_pydantic.py:required_fields"
    ```

=== "Optional Fields"
    ```python
    --8<-- "integrations/e1_02_pydantic.py:optional_fields"
    ```

=== "Field Constraints"
    ```python
    --8<-- "integrations/e1_02_pydantic.py:field_constraints"
    ```

## Push Message Models

FastPubSub provides built-in Pydantic models for handling HTTP push subscriptions:

```python
--8<-- "integrations/e1_02_pydantic.py:push_endpoint"
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
--8<-- "integrations/e1_02_pydantic.py:schema_evolution"
```

### Handling Unknown Fields

```python
--8<-- "integrations/e1_02_pydantic.py:extra_handling"
```

1. Ignore extra fields from newer message versions
2. Fail if message has unexpected fields

## Best Practices

1. **Use Explicit Models:** Define Pydantic models for all message types. This documents your message schema and catches errors early.
2. **Handle Validation Errors:** Always wrap `model_validate_json()` in try/except. Invalid messages should be dropped, not retried forever.
3. **Version Your Schemas:** Use optional fields with defaults when evolving schemas. This maintains backward compatibility during deployments.
4. **Validate at Boundaries:** Validate incoming messages at the handler entry point. Trust your own Pydantic models when publishing.

## Recap

- FastPubSub **automatically serializes** Pydantic models to JSON when publishing
- Use `model_validate_json()` to **parse and validate** incoming messages
- Handle `ValidationError` by **raising Drop** for invalid messages
- Built-in `PushMessage` model handles **HTTP push endpoints**
- Use **optional fields with defaults** for backward-compatible schema evolution
- **Next**: Learn about [Uvicorn Integration](uvicorn.md) for production deployments
