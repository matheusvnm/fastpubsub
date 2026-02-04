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
--8<-- "advanced/e1_05_delivery_guarantees.py:exactly_once"
```

1. Enables exactly-once delivery for this subscription

---

## Step-by-Step

1. Decide if your handler is non-idempotent.
2. Enable `enable_exactly_once_delivery=True` on the subscriber.
3. Add idempotency keys to messages anyway for auditing.
4. Load test to measure latency impact.

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
    Exactly-once delivery requires additional coordination between Pub/Sub and your subscriber. This adds latency and increases costs. Use it only when truly necessary.

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
--8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_handler"
```

1. Use a unique identifier from the message
2. Check your database/cache before processing
3. Record that you've processed this event

### Idempotency Patterns

=== "Database Check"
    ```python
    --8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_database"
    ```

=== "Redis Check"
    ```python
    --8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_redis"
    ```

!!! tip "Prefer Idempotency Over Exactly-Once"
    Idempotent handlers are more robust than exactly-once delivery. They work correctly even if you need to replay messages or switch messaging systems.

## Combining with Other Features

Exactly-once delivery works well with other FastPubSub features:

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:exactly_once_combined"
```

## Best Practices

1. **Include Idempotency Keys**: Even with exactly-once delivery, include unique identifiers in your messages. This helps with debugging and allows you to switch approaches later.

2. **Test Duplicate Handling**: Write tests that verify your handler behaves correctly when receiving the same message twice. This catches issues before production.

3. **Monitor Duplicate Rates**: Track how often duplicates would have occurred. If the rate is very low, you might not need exactly-once delivery at all.

4. **Regional Considerations**: Exactly-once delivery is only available in certain regions. Check Google Cloud documentation for availability in your region.

---

## Common Pitfalls

- Enabling exactly-once for idempotent workloads with no benefit.
- Assuming it removes the need for idempotency keys.
- Ignoring regional availability.

## Recap

- **Exactly-once delivery** guarantees each message is processed once, preventing duplicates
- Enable with `enable_exactly_once_delivery=True` on your subscriber
- **Trade-offs**: Higher latency, lower throughput, increased cost
- **Alternative**: Make handlers idempotent using unique identifiers and database checks
- **Prefer idempotency** when possible - it's more robust and works across systems
- **Next**: Learn about [Message Ordering](ordering.md) to process messages in sequence
