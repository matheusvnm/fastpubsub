---
icon: lucide/eye
---

# Observability and Logging

FastPubSub includes a powerful logging system designed for production environments. You get context-aware logging with minimal setup.

## Built-in Logger

FastPubSub provides a pre-configured logger that you can import and use immediately:

```python
from fastpubsub.logger import logger

@broker.subscriber(...)
async def handle_task(message: Message):
    logger.info("This is a log message!")
```

### Context-Aware Logging

The logger uses Python's `ContextVars` to safely add metadata in async environments. FastPubSub automatically adds context to every message:

- Message ID
- Topic name
- Handler name

Any log inside a subscriber is automatically tagged:

```python
@broker.subscriber(topic_name="orders", alias="order-handler", ...)
async def handle_order(message: Message):
    logger.info("Processing order")
    # Output: Processing order | name=order-handler message_id=12345 topic_name=orders
```

### Custom Context

Add your own context using `logger.contextualize()`:

```python
@broker.subscriber(...)
async def handle_task(message: Message):
    user_id = message.attributes.get("user_id")

    with logger.contextualize(user_id=user_id):
        logger.info("Processing task for user.")
        # ... some work ...
        logger.warn("User processing had a minor issue.")

    logger.info("This log will NOT have the user_id tag.")
```

**Output:**

```
2025-10-25 13:30:00 | INFO | Processing task for user. | message_id=12345 topic_name=tasks user_id=123
2025-10-25 13:30:00 | WARN | User processing had a minor issue. | message_id=12345 topic_name=tasks user_id=123
2025-10-25 13:30:01 | INFO | This log will NOT have the user_id tag. | message_id=12345 topic_name=tasks
```

---

## Structured JSON Logging

For production, switch to structured JSON output for log aggregation platforms:

```bash
# Via environment variable
export FASTPUBSUB_ENABLE_LOG_SERIALIZE=1

# Or via CLI flag
fastpubsub run app:app --log-serialize
```

**JSON Output:**

```json
{
  "timestamp": "2025-10-25 13:30:00,123",
  "level": "INFO",
  "name": "fastpubsub",
  "message": "Processing task for user.",
  "module": "my_app",
  "function": "handle_task",
  "line": 15,
  "message_id": "12345",
  "topic_name": "tasks",
  "user_id": "u_abc"
}
```

---

## Log Levels

Use appropriate log levels:

```python
from fastpubsub.logger import logger

@broker.subscriber(...)
async def handler(message: Message):
    logger.debug("Detailed debug info", extra={"raw_data": message.data})
    logger.info("Processing order", extra={"order_id": order_id})
    logger.warning("Inventory low", extra={"sku": sku, "quantity": qty})
    logger.error("Payment failed", extra={"reason": reason})
    logger.critical("Database unreachable", extra={"attempts": attempts})
```

**Production log level:**

```bash
fastpubsub run app:app --log-level info --log-serialize
```

---

## Health Check Endpoints

FastPubSub provides built-in health checks for orchestration:

### Liveness Probe

Checks if the application is running:

```bash
curl http://localhost:8000/consumers/alive
# Response: 200 OK
```

**Kubernetes configuration:**

```yaml
livenessProbe:
  httpGet:
    path: /consumers/alive
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 5
```

### Readiness Probe

Checks if subscribers are actively polling:

```bash
curl http://localhost:8000/consumers/ready
# Response: 200 OK if subscribers are running
# Response: 503 Service Unavailable if not ready
```

**Kubernetes configuration:**

```yaml
readinessProbe:
  httpGet:
    path: /consumers/ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 3
```

---

## Cloud Logging Integration

For Google Cloud Logging:

```python
import google.cloud.logging
from fastpubsub.logger import logger

# Set up Cloud Logging
client = google.cloud.logging.Client()
client.setup_logging()

# Your code works unchanged
@broker.subscriber(...)
async def handler(message: Message):
    logger.info("Message processed")  # Sent to Cloud Logging
```

---

## Alerting Patterns

### Error Rate Monitoring

Create a middleware to track errors:

```python
from fastpubsub import BaseMiddleware, Message, Middleware
from fastpubsub.logger import logger

class AlertingMiddleware(BaseMiddleware):
    def __init__(self, error_threshold: int = 10):
        super().__init__()
        self.error_threshold = error_threshold
        self.error_count = 0
        self.alert_sent = False

    async def on_message(self, message: Message):
        try:
            return await super().on_message(message)
        except Exception as e:
            self.error_count += 1

            if self.error_count >= self.error_threshold and not self.alert_sent:
                await self.send_alert(f"High error rate: {self.error_count} errors")
                self.alert_sent = True

            raise

    async def send_alert(self, message: str):
        logger.critical(f"ALERT: {message}")
        # Send to PagerDuty, Slack, etc.

broker = PubSubBroker(
    project_id="your-project-id",
    middlewares=[Middleware(AlertingMiddleware, error_threshold=10)]
)
```

### Dead-Letter Queue Monitoring

```python
@broker.subscriber(
    alias="dlq-monitor",
    topic_name="orders-dlq",
    subscription_name="dlq-monitor-subscription",
)
async def monitor_dead_letters(message: Message):
    logger.critical("Message sent to DLQ", extra={
        "message_id": message.id,
        "original_topic": message.attributes.get("original_topic"),
        "failure_count": message.attributes.get("delivery_attempt"),
    })

    await send_alert_to_ops(
        severity="high",
        message=f"DLQ message: {message.id}",
        context=message.attributes
    )
```

---

## Performance Monitoring

### Latency Tracking Middleware

```python
import time
from datetime import datetime, timezone
from fastpubsub import BaseMiddleware, Message
from fastpubsub.logger import logger

class LatencyMiddleware(BaseMiddleware):
    async def on_message(self, message: Message):
        # Calculate message age
        if message.publish_time:
            age_seconds = (datetime.now(timezone.utc) - message.publish_time).total_seconds()
            logger.info(f"Message age: {age_seconds:.2f}s")

            if age_seconds > 300:  # 5 minutes
                logger.warning(f"Old message detected: {age_seconds:.2f}s old")

        # Track processing time
        start_time = time.monotonic()
        result = await super().on_message(message)
        processing_time = (time.monotonic() - start_time) * 1000

        logger.info(f"Processing took {processing_time:.2f}ms")

        return result
```

---

## Debugging Tips

### Enable Debug Logging

```bash
fastpubsub run app:app --log-level debug --log-colorize
```

### Trace Specific Messages

```python
@broker.subscriber(...)
async def handler(message: Message):
    logger.debug("Full message", extra={
        "message_id": message.id,
        "data": message.data.decode("utf-8"),
        "attributes": message.attributes,
        "ordering_key": message.ordering_key,
        "publish_time": message.publish_time.isoformat() if message.publish_time else None,
    })
```

---

## Recap

- **Built-in logger**: Import from `fastpubsub.logger` and use immediately
- **Context-aware**: Automatic `message_id`, `topic_name`, and handler context
- **Custom context**: Use `logger.contextualize()` for additional tags
- **JSON logs**: Enable with `--log-serialize` for production
- **Health checks**: `/consumers/alive` and `/consumers/ready` for orchestration
- **Alerting**: Monitor error rates, DLQs, and performance with middleware
- **Log levels**: Use appropriate levels (debug, info, warning, error, critical)
