---
icon: lucide/filter
---

# Message Filtering

Server-side message filtering lets Pub/Sub deliver only the messages your subscriber needs. This reduces costs, improves performance, and simplifies your handler code.

## Why Use Filtering?

Without filtering, every subscriber on a topic receives every message. Your code must then decide which messages to process and which to ignore. With server-side filtering:

- **Lower costs** - You only pay for messages your subscriber actually receives
- **Better performance** - Less network traffic and processing overhead
- **Cleaner code** - No need for conditional logic to skip irrelevant messages

!!! info "How It Works"
    Pub/Sub evaluates your filter expression against each message's attributes. Messages that match are delivered; messages that don't are skipped. This happens on Google's servers, not in your application.

## Basic Filtering

Add a `filter_expression` parameter to your subscriber to filter messages by their attributes:

```python
--8<-- "advanced/e1_03_filters.py:basic_filter"
```

1. Filter syntax: `attributes.{name} = "{value}"`

## Publishing with Attributes

For filtering to work, you must include attributes when publishing messages:

```python
# This message will reach the order-handler above
await broker.publish(
    topic_name="events",
    data={"order_id": "12345", "amount": 99.99},
    attributes={"event_type": "order", "region": "us-west"},  # (1)!
)

# This message will NOT reach the order-handler
await broker.publish(
    topic_name="events",
    data={"user_id": "abc", "action": "login"},
    attributes={"event_type": "user"},  # (2)!
)
```

1. Matches the filter `attributes.event_type = "order"`
2. Does not match - `event_type` is "user", not "order"

!!! warning "Attributes Are Required for Filtering"
    Messages without attributes will not match any filter expression. Always include relevant attributes when publishing to filtered topics.

---

## Step-by-Step

1. Decide which attributes you will filter on (e.g., `event_type`).
2. Add `filter_expression` to the subscriber.
3. Publish messages with matching attributes.
4. Verify only the expected subscriber receives them.

## Filter Expression Syntax

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equals | `attributes.type = "order"` |
| `!=` | Not equals | `attributes.status != "cancelled"` |
| `>` | Greater than | `attributes.priority > "5"` |
| `<` | Less than | `attributes.amount < "1000"` |
| `>=` | Greater or equal | `attributes.level >= "warning"` |
| `<=` | Less or equal | `attributes.age <= "30"` |

!!! note "All Values Are Strings"
    Pub/Sub attributes are always strings. Numeric comparisons work lexicographically, not numerically. `"9" > "10"` is true because "9" comes after "1" alphabetically.

### Logical Operators

Combine conditions with `AND` and `OR`:

=== "AND (both must match)"
    ```python
    --8<-- "advanced/e1_03_filters.py:filter_and"
    ```

=== "OR (either can match)"
    ```python
    --8<-- "advanced/e1_03_filters.py:filter_or"
    ```

### Checking Attribute Existence

Use `hasPrefix` to check if an attribute exists:

```python
--8<-- "advanced/e1_03_filters.py:filter_has_prefix"
```

1. `hasPrefix(attr, "")` returns true if the attribute exists (any value)

## Multiple Subscribers Pattern

A common pattern is having multiple subscribers on the same topic, each with different filters:

```python
--8<-- "advanced/e1_03_filters.py:multiple_subscribers"
```

!!! tip "Audit Subscribers"
    Create a subscriber without a filter to capture all messages for auditing, analytics, or debugging purposes.

## Testing Filters

Use `PubSubTestClient` to verify your filters work correctly:

```python
import pytest
from fastpubsub import Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient

@pytest.mark.asyncio
async def test_filter_routes_correctly():
    broker = PubSubBroker(project_id="test")
    order_messages: list[Message] = []

    @broker.subscriber(
        alias="order-handler",
        topic_name="events",
        subscription_name="order-sub",
        filter_expression='attributes.event_type = "order"',
    )
    async def handle_orders(msg: Message):
        order_messages.append(msg)

    async with PubSubTestClient(broker) as client:
        # Should be received
        await client.publish(
            {"order_id": "123"},
            topic="events",
            attributes={"event_type": "order"},
        )

        # Should NOT be received
        await client.publish(
            {"user_id": "abc"},
            topic="events",
            attributes={"event_type": "user"},
        )

    assert len(order_messages) == 1
```

??? example "See full testing examples"
    Check out the complete filter testing examples in [snippets/testing/e1_02_filter_expressions.py](../../snippets/testing/e1_02_filter_expressions.py).

## Best Practices

1. **Use Consistent Attribute Names**: Define a standard set of attribute names across your application (e.g., `event_type`, `source`, `priority`). Document them for your team.

2. **Keep Filters Simple**: Complex filter expressions are hard to debug. If you need complex logic, consider splitting into multiple subscribers or handling the logic in your code.

3. **Test Filter Edge Cases**: Test what happens when attributes are missing or have unexpected values. Your filters should handle these gracefully.

4. **Filter Changes May Require New Subscriptions**: Depending on your setup, changing a filter can require creating a new subscription and deleting the old one. Validate this in your environment before deploying.

## Common Pitfalls

- Publishing without attributes (filters never match).
- Overly complex filter expressions that are hard to debug.
- Numeric comparisons on string attributes (`"9" > "10"` is true).

## Recap

- **Filter expressions** let Pub/Sub deliver only matching messages to your subscriber
- Use `attributes.{name} = "{value}"` syntax for basic filtering
- Combine conditions with `AND` and `OR` operators
- **Always include attributes** when publishing to filtered topics
- **Test your filters** with `PubSubTestClient` before deploying
- **Next**: Learn about [Exactly-Once Delivery](delivery-guarantees.md) for critical operations
