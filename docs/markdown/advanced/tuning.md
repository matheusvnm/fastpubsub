---
icon: lucide/sliders
---

# Performance Tuning

Optimize your FastPubSub application for throughput, latency, and resource efficiency. This guide covers the key parameters that affect performance and how to tune them for your workload.

## Key Performance Parameters

FastPubSub provides several parameters to control message processing behavior:

| Parameter | Purpose | Default |
|-----------|---------|---------|
| `max_messages` | Concurrent message limit | 100 |
| `ack_deadline_seconds` | Processing time limit | 60 |
| `min_backoff_delay_secs` | Minimum retry delay | 10 |
| `max_backoff_delay_secs` | Maximum retry delay | 600 |

## Concurrency Control with max_messages

The `max_messages` parameter controls how many messages your subscriber processes simultaneously. This is your primary tool for balancing throughput against resource usage.

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="high-throughput",
    topic_name="events",
    subscription_name="events-subscription",
    max_messages=500,  # (1)!
)
async def high_throughput_handler(message: Message):
    await fast_async_operation(message.data)
```

1. Process up to 500 messages concurrently

### Choosing the Right Value

=== "I/O-Bound Workloads"
    ```python
    # High concurrency for I/O-bound tasks (API calls, database queries)
    @broker.subscriber(
        alias="api-caller",
        topic_name="api-requests",
        subscription_name="api-requests-subscription",
        max_messages=500,  # High - most time is spent waiting
    )
    async def call_external_api(message: Message):
        await http_client.post("/api/endpoint", json=message.data)
    ```

=== "CPU-Bound Workloads"
    ```python
    # Low concurrency for CPU-bound tasks
    @broker.subscriber(
        alias="data-processor",
        topic_name="processing-jobs",
        subscription_name="processing-subscription",
        max_messages=10,  # Low - use multiple workers instead
    )
    async def process_data(message: Message):
        result = compute_heavy_operation(message.data)
        await save_result(result)
    ```

=== "Rate-Limited APIs"
    ```python
    # Match the API rate limit
    @broker.subscriber(
        alias="rate-limited-api",
        topic_name="api-requests",
        subscription_name="api-requests-subscription",
        max_messages=50,  # Match API's rate limit
    )
    async def call_rate_limited_api(message: Message):
        await rate_limited_client.call(message.data)
    ```

### Guidelines by Workload Type

| Workload Type | Recommended `max_messages` | Reason |
|---------------|---------------------------|--------|
| Fast async I/O | 100-1000 | Most time spent waiting; high concurrency is efficient |
| Database queries | 50-200 | Balance throughput with connection pool limits |
| External APIs | Match rate limit | Avoid hitting API rate limits |
| CPU-intensive | 10-50 | Use multiple workers instead of high concurrency |
| Memory-heavy | Lower values | Prevent out-of-memory errors |

!!! tip "Start Conservative, Scale Up"
    Begin with a lower value (50-100) and increase gradually while monitoring memory and CPU usage. It's easier to scale up than to recover from resource exhaustion.

## Processing Time with ack_deadline_seconds

The `ack_deadline_seconds` parameter sets how long Pub/Sub waits before considering a message processing failed. If your handler doesn't acknowledge the message within this time, Pub/Sub redelivers it.

```python
@broker.subscriber(
    alias="slow-processor",
    topic_name="heavy-tasks",
    subscription_name="heavy-tasks-subscription",
    ack_deadline_seconds=600,  # (1)!
    max_messages=10,  # (2)!
)
async def slow_handler(message: Message):
    await complex_ml_inference(message.data)
```

1. Allow up to 10 minutes for processing
2. Low concurrency since each task is resource-intensive

### Choosing the Right Deadline

| Task Duration | Recommended `ack_deadline_seconds` |
|---------------|-----------------------------------|
| < 10 seconds | 30 |
| 10-60 seconds | 60 (default) |
| 1-5 minutes | 300 |
| 5-10 minutes | 600 (maximum) |

!!! warning "Don't Set It Too Low"
    If your deadline is shorter than your average processing time, messages will be redelivered while still being processed, causing duplicates. Always add buffer time.

!!! warning "Maximum Is 600 Seconds"
    Pub/Sub's maximum ack deadline is 600 seconds (10 minutes). If your processing takes longer, break the work into smaller chunks or use a different architecture.

## Retry Backoff Configuration

When messages fail, Pub/Sub retries them with exponential backoff. Configure the backoff behavior to match your failure patterns:

```python
@broker.subscriber(
    alias="api-with-backoff",
    topic_name="api-calls",
    subscription_name="api-calls-subscription",
    min_backoff_delay_secs=10,      # (1)!
    max_backoff_delay_secs=600,     # (2)!
    max_delivery_attempts=10,        # (3)!
    dead_letter_topic="api-calls-dlq",
)
async def call_api(message: Message):
    await external_api.call(message.data)
```

1. First retry waits at least 10 seconds
2. Maximum wait between retries is 10 minutes
3. Give up after 10 attempts

### Backoff Schedule Example

With `min_backoff=10` and `max_backoff=600`:

| Attempt | Approximate Wait |
|---------|------------------|
| 1 | Immediate |
| 2 | ~10 seconds |
| 3 | ~20 seconds |
| 4 | ~40 seconds |
| 5 | ~80 seconds |
| 6 | ~160 seconds |
| 7+ | ~600 seconds (capped) |

### Backoff Strategy by Failure Type

=== "Transient Failures"
    ```python
    # Short backoff for transient issues (network blips)
    @broker.subscriber(
        alias="network-sensitive",
        topic_name="events",
        subscription_name="events-subscription",
        min_backoff_delay_secs=5,
        max_backoff_delay_secs=60,
        max_delivery_attempts=5,
    )
    async def handle_event(message: Message):
        await send_notification(message.data)
    ```

=== "External Service Issues"
    ```python
    # Longer backoff for external service outages
    @broker.subscriber(
        alias="external-api",
        topic_name="api-calls",
        subscription_name="api-calls-subscription",
        min_backoff_delay_secs=30,
        max_backoff_delay_secs=600,
        max_delivery_attempts=10,
    )
    async def call_api(message: Message):
        await external_service.process(message.data)
    ```

## Scaling with Multiple Workers

For CPU-bound workloads, use multiple worker processes instead of high concurrency:

```bash
# Run with 4 worker processes
fastpubsub run myapp:app --workers 4
```

Each worker has its own `max_messages` limit, so total concurrency = `workers × max_messages`.

!!! info "Worker Memory"
    Each worker is a separate process with its own memory space. Monitor total memory usage when running multiple workers.

## Monitoring Performance

Track these metrics to understand your application's performance:

| Metric | What It Tells You |
|--------|-------------------|
| Messages processed/second | Throughput |
| Processing time (p50, p95, p99) | Latency distribution |
| Retry rate | Failure frequency |
| DLT message count | Permanent failures |
| Memory usage | Resource consumption |
| CPU usage | Processing efficiency |

??? example "See concurrency control in action"
    Check out the complete example in [snippets/basic_usage/e5_01_subscribers_max_messages.py](../../snippets/basic_usage/e5_01_subscribers_max_messages.py).

## Complete Tuned Example

A production-ready configuration combining all tuning parameters:

```python
from fastpubsub import FastPubSub, PubSubBroker, Message, Middleware
from fastpubsub import GZipMiddleware

broker = PubSubBroker(
    project_id="your-project-id",
    shutdown_timeout=30.0,  # (1)!
    middlewares=[
        Middleware(GZipMiddleware, compresslevel=6)  # (2)!
    ]
)
app = FastPubSub(broker)

@broker.subscriber(
    alias="optimized-processor",
    topic_name="events",
    subscription_name="events-subscription",
    # Concurrency
    max_messages=200,  # (3)!
    # Timeouts
    ack_deadline_seconds=120,  # (4)!
    # Retry policy
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=300,
    max_delivery_attempts=5,
    # Error handling
    dead_letter_topic="events-dlq",
    autocreate=True,
)
async def process_event(message: Message):
    await handle_event(message.data)
```

1. Wait up to 30 seconds for in-flight messages during shutdown
2. Compress messages to reduce bandwidth
3. Process 200 messages concurrently
4. Allow 2 minutes per message

## Best Practices

!!! tip "Profile Before Tuning"
    Measure your current performance before making changes. You can't improve what you don't measure.

!!! tip "Test Under Load"
    Tune parameters under realistic load conditions. Performance characteristics change significantly between light and heavy load.

!!! tip "Monitor Memory Closely"
    High `max_messages` values increase memory usage. Watch for out-of-memory errors and adjust accordingly.

!!! tip "Use Graceful Shutdown"
    Configure `shutdown_timeout` on your broker to allow in-flight messages to complete before termination.

## Recap

- **`max_messages`** controls concurrent processing - tune based on workload type
- **`ack_deadline_seconds`** sets processing time limit - add buffer for safety
- **Backoff configuration** helps handle transient vs. persistent failures
- **Multiple workers** scale better than high concurrency for CPU-bound tasks
- **Monitor metrics** to guide tuning decisions
- **Next**: Learn about [Cross-Project Configuration](multiple-projects.md) for multi-project setups
