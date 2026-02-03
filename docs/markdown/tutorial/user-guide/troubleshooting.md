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

**Solution:**

```bash
# Full Pub/Sub access
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:SA@PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.editor"
```

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
   from fastpubsub.exceptions import Drop

   @broker.subscriber(...)
   async def handler(message: Message):
       try:
           data = SomeModel.model_validate_json(message.data)
       except ValidationError as e:
           logger.error(f"Invalid message: {e}")
           raise Drop("Invalid message format")  # Don't retry
   ```

2. **Processing timeout (ack deadline exceeded):**
   ```python
   @broker.subscriber(
       alias="slow-handler",
       topic_name="topic",
       subscription_name="subscription",
       ack_deadline_seconds=120,  # Increase deadline
   )
   async def slow_handler(message: Message):
       await asyncio.sleep(60)  # Takes longer than default 60s
   ```

3. **Blocking operations:**
   ```python
   # BAD: Blocks event loop
   time.sleep(5)

   # GOOD: Non-blocking
   await asyncio.sleep(5)
   ```

---

#### Duplicate message processing

**Solutions:**

1. **Make handlers idempotent:**
   ```python
   @broker.subscriber(...)
   async def idempotent_handler(message: Message):
       event_id = message.attributes.get("event_id")

       if await redis.exists(f"processed:{event_id}"):
           return  # Already handled

       await do_work(message.data)
       await redis.set(f"processed:{event_id}", "1", ex=86400)
   ```

2. **Enable exactly-once delivery:**
   ```python
   @broker.subscriber(
       alias="handler",
       topic_name="topic",
       subscription_name="subscription",
       enable_exactly_once_delivery=True,
   )
   async def handler(message: Message):
       pass
   ```

---

### Performance Issues

#### High latency / slow processing

**Solutions:**

1. **Increase `max_messages`:**
   ```python
   @broker.subscriber(
       alias="handler",
       topic_name="topic",
       subscription_name="subscription",
       max_messages=500,  # Higher concurrency for I/O-bound tasks
   )
   async def handler(message: Message):
       await fast_operation(message.data)
   ```

2. **Profile with middleware:**
   ```python
   import time
   from fastpubsub import BaseMiddleware, Message

   class ProfilingMiddleware(BaseMiddleware):
       async def on_message(self, message: Message):
           start = time.monotonic()
           result = await super().on_message(message)
           duration = (time.monotonic() - start) * 1000
           logger.info(f"Message {message.id} took {duration:.2f}ms")
           return result
   ```

---

#### High memory usage

**Solutions:**

1. **Limit concurrent messages:**
   ```python
   @broker.subscriber(
       alias="handler",
       topic_name="topic",
       subscription_name="subscription",
       max_messages=10,  # Lower for memory-intensive tasks
   )
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
   broker = PubSubBroker(
       project_id="your-project-id",
       shutdown_timeout=30.0,  # Wait 30s for in-flight messages
   )
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
@broker.subscriber(
    alias="ordered-handler",
    topic_name="events",
    subscription_name="events-subscription",
    enable_message_ordering=True,
)
async def handler(message: Message):
    user_id = message.ordering_key
    await process_in_order(user_id, message.data)
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
