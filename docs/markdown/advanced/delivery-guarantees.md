---
icon: lucide/shield-check
---

# Exactly-Once Delivery

Exactly-once delivery aims to eliminate duplicate processing for a subscription.
In FastPubSub, it is configured through `enable_exactly_once_delivery=True` on the subscriber.

This capability should be treated as a domain-level decision, not a default optimization.
It introduces additional coordination cost and changes throughput characteristics.

## Delivery Semantics in Context

| Model | Guarantee | Typical Usage |
|------|-----------|---------------|
| At-least-once | Message may be delivered multiple times | Most event systems with idempotent handlers |
| Exactly-once | Message is processed without duplicate delivery | Duplicate-sensitive operations |

By default, Pub/Sub uses at-least-once delivery.
That default is usually sufficient when handlers are idempotent.

## Enabling Exactly-Once

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:exactly_once"
```

The configuration is explicit and local to the subscriber.
You can apply it selectively to critical flows rather than globally.

## Decision Framework

Enable exactly-once when all of the following are true:

- Duplicate side effects are expensive or unacceptable.
- Handler idempotency is hard to guarantee for business reasons.
- Added latency and cost are acceptable for that workflow.

Avoid enabling exactly-once when:

- Handlers are naturally idempotent.
- Throughput is more important than strict single-processing semantics.
- The flow is observational (analytics, telemetry) and duplicates are tolerable.

## Engineering Trade-offs

| Dimension | At-Least-Once | Exactly-Once |
|----------|----------------|--------------|
| Throughput | Higher | Lower |
| Latency | Lower | Higher |
| Cost | Lower | Higher |
| Duplicate protection | Application concern | Broker-level guarantee |

!!! warning "Exactly-Once Is Not a Universal Default"
    Use it for operations where duplicate effects are materially harmful.
    For general event processing, idempotent handlers are often a better long-term baseline.

## Idempotency as a Robust Alternative

FastPubSub supports straightforward idempotent designs without enabling exactly-once.

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_handler"
```

### Database Constraint Pattern

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_database"
```

### Cache/Key Store Pattern

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:idempotent_redis"
```

These patterns usually remain valid across brokers and replay workflows.

## Composition with Other Reliability Controls

Critical subscriptions normally combine delivery guarantees with dead-letter and retry policy:

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:exactly_once_combined"
```

This prevents a strict delivery model from masking unresolved failure classes.

## Validation with `PubSubTestClient`

For local validation, test idempotency behavior directly in the handler path.

```python
--8<-- "advanced/e1_05_delivery_guarantees.py:idempotency_test_client"
```

`PubSubTestClient` does not emulate full managed delivery internals.
It is best used to verify deterministic application behavior when duplicate inputs occur.


## Design Recommendations

### Keep Idempotency Keys Even with Exactly-Once

Include explicit identifiers such as `event_id` in message attributes or payload.
They support diagnostics, replay logic, and broker migration scenarios.
Exactly-once behavior in managed Pub/Sub has platform constraints and should be validated against current Google guidance.
For details, see [Google Pub/Sub: Exactly-Once Delivery](https://docs.cloud.google.com/pubsub/docs/exactly-once-delivery).

### Measure Before and After Enabling

Track latency percentiles and throughput for the specific subscription.
Exactly-once should be justified by measurable risk reduction.

### Apply Selectively

Use per-subscriber granularity:

- Enable on billing, payment, and irreversible state transitions.
- Keep at-least-once for high-volume, idempotent, or analytical streams.


## Recap

- Exactly-once is a targeted reliability feature for duplicate-sensitive domains.
- Enable it with `enable_exactly_once_delivery=True` on the subscriber.
- Expect latency and cost trade-offs.
- Prefer explicit idempotency where practical and portable.
- Validate handler behavior early with `PubSubTestClient`; validate managed semantics in integration.
