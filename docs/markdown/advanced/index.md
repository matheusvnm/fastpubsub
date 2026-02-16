---
icon: lucide/flask-conical
---

# Advanced Usage

This section consolidates FastPubSub capabilities that are typically adopted after the core development workflow is already stable.
The material is organized as an engineering reference for reliability, consistency, and operational scalability.

The [User Guide](../tutorial/index.md) remains the recommended starting point for all teams.
Advanced Usage should be treated as a continuation layer for systems that now require stricter delivery semantics,
explicit failure controls, or multi-project architecture.

## Scope of This Section

The advanced pages are split into two categories:

- **Google Pub/Sub semantics**: subscription-level behavior such as dead-letter routing, filtering, ordering, and exactly-once delivery.
- **FastPubSub architecture controls**: middleware composition, cross-project topology, and throughput tuning.

This separation helps when reviewing incidents or designing new flows, because it makes clear whether a behavior is driven by
managed Pub/Sub configuration or by application-level FastPubSub decisions.

## Recommended Reading Order

If you are applying these topics for the first time, follow this sequence:

1. [Dead-Letter Topics](dlt.md) for bounded failure handling.
2. [Message Filtering](filters.md) for selective delivery and subscription cost control.
3. [Exactly-Once Delivery](delivery-guarantees.md) for duplicate-sensitive flows.
4. [Message Ordering](ordering.md) for sequence-dependent domains.
5. [Custom Middlewares](custom-middlewares.md) for application-side control points.
6. [Cross-Project Configuration](multiple-projects.md) for multi-project boundaries.
7. [Performance Tuning](tuning.md) for production throughput and latency optimization.

## Methodology Used in These Pages

Each guide follows a consistent structure:

- A conceptual model that defines what the mechanism guarantees and what it does not guarantee.
- API-focused configuration examples using external snippets under `docs/snippets/`.
- Validation-oriented examples using `PubSubTestClient` when behavior can be asserted locally.
- Operational caveats and anti-patterns that usually appear in production systems.

## Assumptions

The guides assume that you already know:

- How to define subscribers and publishers with FastPubSub.
- How topic and subscription naming works in your environment.
- Basic async programming principles (`async def`, `await`, non-blocking I/O).

If any of those are still unclear, return to:

- [First Steps](../tutorial/user-guide/first-steps.md)
- [Subscribers](../tutorial/user-guide/subscribers.md)
- [Publishers](../tutorial/user-guide/publishers.md)
- [Middlewares](../tutorial/user-guide/middlewares.md)
