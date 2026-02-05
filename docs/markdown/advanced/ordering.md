---
icon: lucide/list-ordered
---

# Message Ordering

Message ordering ensures that messages with the same ordering key are processed in the exact sequence they were published. This is essential when the order of operations matters, like processing a user's actions chronologically.

## Why Order Matters

Consider a user updating their profile:

1. User changes email to `new@example.com`
2. User changes email to `final@example.com`

Without ordering, message 2 might be processed before message 1, leaving the user with the wrong email. Message ordering prevents this.

!!! info "Ordering Key Scope"
    Ordering is guaranteed only for messages with the same ordering key. Messages with different keys can still be processed in parallel and out of order relative to each other.

## How Ordering Works

```mermaid
sequenceDiagram
    participant P as Publisher
    participant T as Topic
    participant S as Subscriber

    P->>T: Message A (key: user-1)
    P->>T: Message B (key: user-1)
    P->>T: Message C (key: user-2)

    Note over S: Processes A before B (same key)
    Note over S: C can process anytime (different key)
```

## Enabling Message Ordering

### Subscriber Configuration

Enable ordering on your subscriber:

```python
--8<-- "advanced/e1_04_ordering.py:ordered_subscriber"
```

1. Enables ordered delivery for this subscription
2. Access the ordering key from the message

### Publisher Configuration

The publisher must also have ordering enabled:

```python
--8<-- "advanced/e1_04_ordering.py:ordered_publisher"
```

1. Publisher must have ordering enabled to use ordering keys
2. First message for user-123
3. Guaranteed to be processed after the login message

!!! warning "Both Sides Must Enable Ordering"
    If the publisher sends messages with ordering keys but the subscriber doesn't have `enable_message_ordering=True`, messages may still arrive out of order.

---

## Step-by-Step

1. Enable ordering on the subscriber (`enable_message_ordering=True`).
2. Enable ordering on the publisher and send an `ordering_key`.
3. Publish multiple messages with the same key.
4. Verify the handler sees them in sequence.

## Choosing Good Ordering Keys

The ordering key determines which messages are ordered together. Choose keys that group related operations:

=== "User-Scoped"
    ```python
    # All events for a user are ordered
    await publisher.publish(
        data={"action": "purchase", "item": "widget"},
        ordering_key=f"user-{user_id}"
    )
    ```

=== "Resource-Scoped"
    ```python
    # All updates to an order are ordered
    await publisher.publish(
        data={"status": "shipped"},
        ordering_key=f"order-{order_id}"
    )
    ```

=== "Account-Scoped"
    ```python
    # All transactions for an account are ordered
    await publisher.publish(
        data={"type": "credit", "amount": 100},
        ordering_key=f"account-{account_id}"
    )
    ```

### Good Ordering Keys

| Use Case | Ordering Key | Why |
|----------|--------------|-----|
| User actions | `user-{id}` | Actions for one user are ordered |
| Order updates | `order-{id}` | Status changes happen in sequence |
| Inventory | `sku-{id}` | Stock updates for one item are ordered |
| Account transactions | `account-{id}` | Balance updates are sequential |

### Bad Ordering Keys

!!! danger "Avoid These Patterns"
    - **Single global key** (`"all-messages"`) - Forces sequential processing of everything
    - **Too granular** (`"msg-{uuid}"`) - No messages share a key, ordering is useless
    - **Time-based** (`"2024-01-15"`) - Too many messages per key

## Performance Considerations

Ordering reduces parallelism. Messages with the same ordering key are processed sequentially, not concurrently.

```mermaid
graph LR
    subgraph "Without Ordering"
        A1[Msg A] --> P1[Process]
        B1[Msg B] --> P2[Process]
        C1[Msg C] --> P3[Process]
    end

    subgraph "With Ordering (same key)"
        A2[Msg A] --> P4[Process] --> B2[Msg B] --> P5[Process] --> C2[Msg C]
    end
```

!!! tip "Balance Ordering and Throughput"
    Use ordering only when necessary. If messages are independent, let them process in parallel for better throughput.

## Handling Failures with Ordering

When a message fails, subsequent messages with the same ordering key are blocked until the failed message is resolved (retried successfully or moved to dead-letter).

```python
--8<-- "advanced/e1_04_ordering.py:ordered_with_dlt"
```

1. Failed messages go to DLT after max attempts, unblocking the queue

!!! warning "Blocked Message Queues"
    If a message fails repeatedly without dead-letter handling, all subsequent messages with the same ordering key will be blocked indefinitely. Always configure dead-letter topics for ordered subscriptions.

---

## Common Pitfalls

- Using a single global ordering key (kills parallelism).
- Using a unique key per message (no ordering benefits).
- Missing a dead-letter topic on ordered subscriptions.

## Use Cases

=== "User Sessions"
    ```python
    --8<-- "advanced/e1_04_ordering.py:usecase_sessions"
    ```

=== "State Machines"
    ```python
    --8<-- "advanced/e1_04_ordering.py:usecase_state_machine"
    ```

=== "Inventory Updates"
    ```python
    --8<-- "advanced/e1_04_ordering.py:usecase_inventory"
    ```

## Best Practices

1. **Combine with Dead-Letter Topics**: Always use dead-letter topics with ordered subscriptions to prevent blocked queues from stalled messages.

2. **Keep Processing Fast**: Slow processing of one message blocks all subsequent messages with the same key. Keep handlers fast or move heavy work to background tasks.

3. **Monitor Queue Depth**: Track how many messages are waiting per ordering key. A growing queue indicates processing bottlenecks.

4. **Test Ordering Behavior**: Write tests that verify your handler processes messages in the correct order. Send multiple messages and check the final state.

## Recap

- **Message ordering** guarantees messages with the same ordering key are processed sequentially
- Enable on **both publisher** (`enable_message_ordering=True`) and **subscriber**
- Choose **meaningful ordering keys** (user_id, order_id, account_id)
- **Trade-off**: Ordering reduces parallelism - use only when order matters
- **Always use dead-letter topics** to prevent blocked queues
- **Next**: Learn about [Performance Tuning](tuning.md) to optimize throughput
