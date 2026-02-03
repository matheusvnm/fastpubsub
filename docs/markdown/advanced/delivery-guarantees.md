---
icon: lucide/shield-check
---

# Exactly-Once Delivery

Exactly-once delivery guarantees that each message is processed exactly one time, even when failures occur. This prevents duplicate processing but comes with trade-offs in latency and cost.

## Understanding Delivery Semantics

Pub/Sub offers different delivery guarantees. Understanding them helps you choose the right approach for your use case:

| Delivery Type | Guarantee | Use Case |
|--------------|-----------|----------|
| **At-least-once** | Message delivered one or more times | Default, good for idempotent handlers |
| **Exactly-once** | Message delivered exactly one time | Financial transactions, non-idempotent operations |

!!! info "At-Least-Once Is the Default"
    By default, Pub/Sub uses at-least-once delivery. Messages may be delivered multiple times if acknowledgments are lost or your handler crashes mid-processing.

## Enabling Exactly-Once Delivery

Add `enable_exactly_once_delivery=True` to your subscriber:

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="payment-processor",
    topic_name="payments",
    subscription_name="payments-subscription",
    enable_exactly_once_delivery=True,  # (1)!
    autocreate=True,
)
async def process_payment(message: Message):
    # Guaranteed to run exactly once per message
    await charge_customer(message.data)
```

1. Enables exactly-once delivery for this subscription

## When to Use Exactly-Once

=== "Use It For"
    - **Financial transactions** - Charging customers, transferring funds
    - **Inventory updates** - Decrementing stock counts
    - **Sending notifications** - Emails, SMS where duplicates annoy users
    - **Non-idempotent operations** - Actions that can't safely be repeated

=== "Avoid It For"
    - **Idempotent handlers** - If duplicates don't matter, skip the overhead
    - **Low-latency requirements** - Adds coordination latency
    - **High-volume processing** - Increases costs and reduces throughput
    - **Analytics/logging** - Duplicate log entries are usually acceptable

!!! warning "Performance Impact"
    Exactly-once delivery requires additional coordination between Pub/Sub and your subscriber. This adds latency (typically 10-50ms) and increases costs. Use it only when truly necessary.

## Trade-offs Comparison

| Aspect | At-Least-Once | Exactly-Once |
|--------|---------------|--------------|
| **Latency** | Lower | Higher (+10-50ms) |
| **Throughput** | Higher | Lower |
| **Cost** | Lower | Higher |
| **Complexity** | Handler must be idempotent | Simpler handler logic |
| **Reliability** | May process duplicates | No duplicates |

## The Idempotent Alternative

Instead of relying on exactly-once delivery, you can make your handlers idempotent. An idempotent handler produces the same result whether called once or multiple times with the same input.

```python
@broker.subscriber(
    alias="idempotent-handler",
    topic_name="events",
    subscription_name="events-subscription",
    # No exactly-once needed - handler is idempotent
)
async def idempotent_handler(message: Message):
    event_id = message.attributes.get("event_id")  # (1)!

    # Check if already processed
    if await is_already_processed(event_id):  # (2)!
        logger.info(f"Event {event_id} already processed, skipping")
        return

    await process_event(message.data)
    await mark_as_processed(event_id)  # (3)!
```

1. Use a unique identifier from the message
2. Check your database/cache before processing
3. Record that you've processed this event

### Idempotency Patterns

=== "Database Check"
    ```python
    async def idempotent_handler(message: Message):
        order_id = message.data["order_id"]

        # Use database transaction with unique constraint
        try:
            await db.execute(
                "INSERT INTO processed_orders (order_id) VALUES (?)",
                [order_id]
            )
        except UniqueViolationError:
            return  # Already processed

        await fulfill_order(message.data)
    ```

=== "Redis Check"
    ```python
    async def idempotent_handler(message: Message):
        event_id = message.attributes.get("event_id")

        # Set with NX (only if not exists), expire after 24 hours
        was_set = await redis.set(
            f"processed:{event_id}",
            "1",
            nx=True,
            ex=86400
        )

        if not was_set:
            return  # Already processed

        await process_event(message.data)
    ```

=== "Upsert Pattern"
    ```python
    async def idempotent_handler(message: Message):
        user_id = message.data["user_id"]
        new_balance = message.data["balance"]

        # Upsert is naturally idempotent
        await db.execute("""
            INSERT INTO user_balances (user_id, balance)
            VALUES (?, ?)
            ON CONFLICT (user_id) DO UPDATE SET balance = ?
        """, [user_id, new_balance, new_balance])
    ```

!!! tip "Prefer Idempotency Over Exactly-Once"
    Idempotent handlers are more robust than exactly-once delivery. They work correctly even if you need to replay messages or switch messaging systems.

## Combining with Other Features

Exactly-once delivery works well with other FastPubSub features:

```python
@broker.subscriber(
    alias="critical-payment",
    topic_name="payments",
    subscription_name="payments-subscription",
    # Delivery guarantee
    enable_exactly_once_delivery=True,
    # Error handling
    dead_letter_topic="payments-dlq",
    max_delivery_attempts=5,
    # Backoff for transient failures
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=300,
    autocreate=True,
)
async def process_critical_payment(message: Message):
    await charge_customer(message.data)
```

## Best Practices

1. **Include Idempotency Keys**: Even with exactly-once delivery, include unique identifiers in your messages. This helps with debugging and allows you to switch approaches later.

2. **Test Duplicate Handling**: Write tests that verify your handler behaves correctly when receiving the same message twice. This catches issues before production.

3. **Monitor Duplicate Rates**: Track how often duplicates would have occurred. If the rate is very low, you might not need exactly-once delivery at all.

4. **Regional Considerations**: Exactly-once delivery is only available in certain regions. Check Google Cloud documentation for availability in your region.

## Recap

- **Exactly-once delivery** guarantees each message is processed once, preventing duplicates
- Enable with `enable_exactly_once_delivery=True` on your subscriber
- **Trade-offs**: Higher latency, lower throughput, increased cost
- **Alternative**: Make handlers idempotent using unique identifiers and database checks
- **Prefer idempotency** when possible - it's more robust and works across systems
- **Next**: Learn about [Message Ordering](ordering.md) to process messages in sequence
