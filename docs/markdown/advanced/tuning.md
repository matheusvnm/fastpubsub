---
icon: lucide/sliders
---

# Performance Tuning

Performance tuning in FastPubSub is the process of matching subscription and runtime parameters to workload shape.
The objective is not maximum raw throughput in isolation, but stable throughput with predictable latency and bounded resource usage.

## Control Surface Overview

| Parameter | Scope | Primary Effect |
|----------|-------|----------------|
| `max_messages` | Subscriber client-side | Concurrency and memory pressure |
| `ack_deadline_seconds` | Subscription server-side | Processing window before redelivery |
| `min_backoff_delay_secs` / `max_backoff_delay_secs` | Subscription server-side | Retry pacing under failure |
| `max_delivery_attempts` + `dead_letter_topic` | Subscription server-side | Bounded failure handling |
| `shutdown_timeout` | Broker runtime | Graceful drain on shutdown |

## Tuning Workflow

Apply tuning in a controlled loop:

1. Measure baseline metrics (latency, throughput, failure rates, memory).
2. Change one major parameter group at a time.
3. Re-run under representative load.
4. Keep changes that improve SLOs and rollback regressions.

## Concurrency with `max_messages`

`max_messages` sets the upper bound of in-flight messages per subscriber task.

```python
--8<-- "advanced/e1_06_tuning.py:high_concurrency"
```

A compact load-probe setup for this parameter:

```python
--8<-- "basic_usage/e3_02_subscribers_max_messages.py:subscriber_max_messages"

--8<-- "basic_usage/e3_02_subscribers_max_messages.py:bulk_publish"
```

### Workload-Specific Profiles

#### I/O-Bound Workloads

```python
--8<-- "advanced/e1_06_tuning.py:io_bound"
```

#### CPU-Bound Workloads

```python
--8<-- "advanced/e1_06_tuning.py:cpu_bound"
```

#### Rate-Limited Dependencies

```python
--8<-- "advanced/e1_06_tuning.py:rate_limited"
```

### Practical Heuristics

| Workload | Typical `max_messages` Range | Rationale |
|----------|------------------------------|-----------|
| Async I/O dominant | 100-1000 | Waiting time dominates, concurrency pays off |
| DB-bound | 50-200 | Align with pool limits and transaction pressure |
| CPU-heavy | 10-50 | Prefer process scaling over coroutine fan-out |
| External API quotas | Depends on quota | Prevent downstream throttling cascades |

## Acknowledgment Deadline

`ack_deadline_seconds` must exceed expected processing time with safety margin.

```python
--8<-- "advanced/e1_06_tuning.py:ack_deadline"
```

If configured too low, valid in-flight work can be redelivered before completion, creating duplicate processing pressure.

| Expected Processing Time | Suggested `ack_deadline_seconds` |
|--------------------------|----------------------------------|
| < 10 seconds | 30 |
| 10-60 seconds | 60 |
| 1-5 minutes | 300 |
| 5-10 minutes | 600 |

!!! warning "Upper Bound"
    Pub/Sub caps acknowledgment deadline at 600 seconds.
    Workloads beyond this window should be decomposed into smaller steps.

## Retry Backoff and Failure Pacing

Tune retry policy to the failure mode.

```python
--8<-- "advanced/e1_06_tuning.py:retry_backoff"
```

### Short Backoff for Transient Instability

```python
--8<-- "advanced/e1_06_tuning.py:transient_backoff"
```

### Long Backoff for External Outages

```python
--8<-- "advanced/e1_06_tuning.py:external_backoff"
```

Longer backoff reduces retry storms and protects downstream dependencies during partial outages.

## Multi-Process Scaling

For CPU-constrained workloads, scale workers before inflating `max_messages`.

```bash
fastpubsub run myapp:app --workers 4
```

Effective theoretical concurrency is:

`workers x max_messages`

Monitor total memory because each worker process has independent state.

## Integrated Configuration Example

```python
--8<-- "advanced/e1_06_tuning.py:complete_tuned"
```

This profile combines graceful shutdown, `GZipMiddleware`, bounded retries, and dead-letter isolation.

## Metrics to Observe During Tuning

Track these continuously while iterating:

- Throughput (`messages/s`).
- Latency distribution (`p50`, `p95`, `p99`).
- Retry rate and retry burst shape.
- Dead-letter ingress count.
- Worker CPU and memory saturation.

## Validation Approach

For behavior-level checks, `PubSubTestClient` can validate handler correctness.
For performance conclusions, rely on controlled load tests because in-memory tests do not represent broker/network pressure.

## Common Failure Modes

- Increasing `max_messages` without observing memory growth.
- Setting acknowledgment deadline below realistic processing time.
- Applying CPU-bound profile to I/O-bound workload (or inverse).
- Using aggressive retry with no dead-letter strategy.

## Recap

- Tune for workload characteristics, not generic defaults.
- Start with `max_messages`, then align deadlines and retry policy.
- Use dead-letter and backoff to bound and shape failures.
- Scale by workers for CPU pressure, not coroutine count alone.
- Validate behavior with tests and validate performance with load measurement.
