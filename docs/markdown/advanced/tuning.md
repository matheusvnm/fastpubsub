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

---

## Step-by-Step

1. Measure baseline throughput and latency.
2. Tune `max_messages` based on workload type.
3. Adjust `ack_deadline_seconds` for processing time.
4. Configure backoff and dead-letter topics for failures.
5. Re-test under load.

## Concurrency Control with max_messages

The `max_messages` parameter controls how many messages your subscriber processes simultaneously. This is your primary tool for balancing throughput against resource usage.

```python
--8<-- "advanced/e1_06_tuning.py:high_concurrency"
```

1. Process up to 500 messages concurrently

### Choosing the Right Value

=== "I/O-Bound Workloads"
    ```python
    --8<-- "advanced/e1_06_tuning.py:io_bound"
    ```

=== "CPU-Bound Workloads"
    ```python
    --8<-- "advanced/e1_06_tuning.py:cpu_bound"
    ```

=== "Rate-Limited APIs"
    ```python
    --8<-- "advanced/e1_06_tuning.py:rate_limited"
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
--8<-- "advanced/e1_06_tuning.py:ack_deadline"
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
    --8<-- "advanced/e1_06_tuning.py:transient_backoff"
    ```

=== "External Service Issues"
    ```python
    --8<-- "advanced/e1_06_tuning.py:external_backoff"
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
--8<-- "advanced/e1_06_tuning.py:complete_tuned"
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

---

## Common Pitfalls

- Increasing `max_messages` without monitoring memory.
- Setting `ack_deadline_seconds` lower than typical processing time.
- Treating CPU-bound workloads as I/O-bound.

## Recap

- **`max_messages`** controls concurrent processing - tune based on workload type
- **`ack_deadline_seconds`** sets processing time limit - add buffer for safety
- **Backoff configuration** helps handle transient vs. persistent failures
- **Multiple workers** scale better than high concurrency for CPU-bound tasks
- **Monitor metrics** to guide tuning decisions
- **Next**: Learn about [Cross-Project Configuration](multiple-projects.md) for multi-project setups
