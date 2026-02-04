---
icon: lucide/wrench
---

# Troubleshooting and FAQ

This guide helps you resolve common issues when working with FastPubSub.

## Common Issues

### Authentication and Credentials

#### "Could not load the default credentials"

**Error:**
```
google.auth.exceptions.DefaultCredentialsError: Could not automatically determine credentials.
```

**Solutions:**

1. **For local development with emulator:**
   ```bash
   export PUBSUB_EMULATOR_HOST="localhost:8085"
   ```

2. **For production/cloud:**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

3. **Verify credentials:**
   ```python
   from google.auth import default

   credentials, project = default()
   print(f"Using project: {project}")
   ```

---

#### "Permission denied" errors

**Error:**
```
google.api_core.exceptions.PermissionDenied: 403 User not authorized
```

**Required permissions:**

| Action | Required Permissions |
|--------|---------------------|
| Subscribers | `pubsub.subscriptions.consume`, `pubsub.subscriptions.get` |
| Publishers | `pubsub.topics.publish` |
| Autocreate | `pubsub.topics.create`, `pubsub.subscriptions.create` |

**Solution:** Check if you Service Account has enough permissions.

---

### Message Delivery Issues

#### Messages not being consumed

**Debugging steps:**

1. **Check if subscriber is running:**
   ```bash
   fastpubsub run app:app --log-level debug
   # Look for "handler is waiting for messages"
   ```

2. **Verify topic and subscription exist:**
   ```bash
   gcloud pubsub topics list --project=PROJECT_ID
   gcloud pubsub subscriptions list --project=PROJECT_ID
   ```

3. **Check subscription is attached to correct topic:**
   ```bash
   gcloud pubsub subscriptions describe SUBSCRIPTION_NAME --project=PROJECT_ID
   ```

4. **Test with manual publish:**
   ```bash
   gcloud pubsub topics publish TOPIC_NAME --message="test" --project=PROJECT_ID
   ```

5. **Check filter expressions:** If using `filter_expression`, ensure published messages have matching attributes.

---

#### Messages being nacked repeatedly

**Common causes:**

1. **Unhandled exceptions:**
   ```python
   --8<-- "troubleshooting/e1_01_common_patterns.py:validation_error_handling"
   ```

2. **Processing timeout (ack deadline exceeded):**
   ```python
   --8<-- "troubleshooting/e1_01_common_patterns.py:ack_deadline_handler"
   ```

3. **Blocking operations:**
   ```python
   # BAD: Blocks event loop
   time.sleep(5)

   # GOOD: Non-blocking
   await asyncio.sleep(5)
   ```

See [Message Lifecycle](lifecycle.md) for Drop/Retry behavior and [Performance Tuning](../../advanced/tuning.md) for ack deadline guidance.

---

#### Duplicate message processing

**Solutions:**

1. **Make handlers idempotent:**
   ```python
   --8<-- "troubleshooting/e1_01_common_patterns.py:idempotent_handler"
   ```

2. **Enable exactly-once delivery:**
   ```python
   --8<-- "troubleshooting/e1_01_common_patterns.py:exactly_once_handler"
   ```

---

### Performance Issues

#### High latency / slow processing

**Solutions:**

1. **Increase `max_messages`:**
   ```python
   --8<-- "troubleshooting/e1_02_performance_patterns.py:high_throughput_handler"
   ```

2. **Profile with middleware:**
   ```python
   --8<-- "troubleshooting/e1_02_performance_patterns.py:profiling_middleware"
   ```

See [Performance Tuning](../../advanced/tuning.md) for guidance on `max_messages` and `ack_deadline_seconds`.

---

#### High memory usage

**Solutions:**

1. **Limit concurrent messages:**
   ```python
   --8<-- "troubleshooting/e1_02_performance_patterns.py:low_memory_handler"
   ```

2. **Avoid global mutable state:**
   ```python
   # BAD: Memory leak
   all_messages = []

   @broker.subscriber(...)
   async def handler(message: Message):
       all_messages.append(message)  # Never cleared!
   ```

---

### Graceful Shutdown Issues

#### Messages lost during shutdown

**Solutions:**

1. **Increase shutdown timeout:**
   ```python
   --8<-- "troubleshooting/e1_02_performance_patterns.py:shutdown_timeout_broker"
   ```

2. **In Kubernetes, set adequate termination period:**
   ```yaml
   terminationGracePeriodSeconds: 45  # > shutdown_timeout
   ```

---

### Development Issues

#### Emulator not connecting

**Error:**
```
Failed to connect to localhost:8085
```

**Solutions:**

1. **Start the emulator:**
   ```bash
   docker compose up -d pubsub-emulator
   ```

2. **Verify it's running:**
   ```bash
   curl http://localhost:8085
   ```

3. **Set environment variable:**
   ```bash
   export PUBSUB_EMULATOR_HOST="localhost:8085"
   ```

4. **Check port conflicts:**
   ```bash
   lsof -i :8085
   ```

---

## Frequently Asked Questions

### What's the difference between a topic and a subscription?

- **Topic**: A named channel where messages are published
- **Subscription**: A named consumer of messages from a topic
- One topic can have multiple subscriptions (fan-out pattern)
- Each subscription receives a copy of every message

---

### What happens if my handler raises an exception?

| Exception | Action | Message Destiny |
|-----------|--------|--------------|
| None (success) | `ack()` | Removed |
| `Drop` | `ack()` | Removed |
| `Retry` | `nack()` | Redelivered |
| Any other | `nack()` | Redelivered |

---

### How do I process messages in order?

Enable message ordering and use ordering keys:

```python
--8<-- "troubleshooting/e1_02_performance_patterns.py:ordered_handler"
```

---

### Can I use FastPubSub without FastAPI?

Currently, FastPubSub has tight coupling with FastAPI. The core `PubSubBroker` functionality doesn't strictly require FastAPI, but the framework is designed to work with it. Standalone usage is planned for future releases.

---

### How do I test without the emulator?

Use `PubSubTestClient`:

```python
from fastpubsub.testing import PubSubTestClient

async def test_handler():
    async with PubSubTestClient(broker) as client:
        await client.publish("topic", data=b"test")
```

---

### What's the maximum message size?

Google Pub/Sub has a 10MB limit. For larger data:

1. Store data in Cloud Storage and publish the URL
2. Split into multiple messages
3. Use compression (GZipMiddleware)

```python
from fastpubsub import Middleware, GZipMiddleware

broker = PubSubBroker(
    project_id="your-project-id",
    middlewares=[Middleware(GZipMiddleware, compresslevel=6)]
)
```

---

### How do I handle deployments without losing messages?

Use graceful shutdown:

```python
broker = PubSubBroker(
    project_id="your-project-id",
    shutdown_timeout=30.0,
)
```

```yaml
# In Kubernetes
terminationGracePeriodSeconds: 45
```

---

## Getting Help

If you encounter an issue not covered here:

1. **Check logs** with `--log-level debug`
2. **Search existing issues**: [GitHub Issues](https://github.com/matheusvnm/fastpubsub/issues)
3. **Create a minimal reproduction** and file an issue with:
   - FastPubSub version (`pip show fastpubsub`)
   - Python version
   - Error messages and stack traces
   - Relevant code snippets

---

## Recap

- **Authentication**: Set `PUBSUB_EMULATOR_HOST` for local, `GOOGLE_APPLICATION_CREDENTIALS` for cloud
- **Message delivery**: Check autocreate, filters, and ack deadlines
- **Performance**: Tune `max_messages` based on workload
- **Graceful shutdown**: Set adequate `shutdown_timeout`
- **Testing**: Use `PubSubTestClient` for fast unit tests
- **Production**: Use idempotent handlers, monitoring, and proper IAM permissions
