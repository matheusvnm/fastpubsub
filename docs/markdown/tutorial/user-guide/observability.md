---
icon: lucide/eye
---

# Observability and Logging

FastPubSub includes a powerful logging system designed for production environments. You get context-aware logging with minimal setup.

## Built-in Logger

FastPubSub provides a pre-configured logger that you can import and use immediately:

```python
--8<-- "observability/e1_01_logging_basics.py:basic_logger"
```

### Context-Aware Logging

The logger uses Python's `ContextVars` to safely add metadata in async environments. FastPubSub automatically adds context to every message:

- Message ID
- Topic name
- Subscriber handler name

However, you can also add you own context using `logger.contextualize()`:

```python
--8<-- "observability/e1_01_logging_basics.py:custom_context"
```

**Output:**

```
2025-10-25 13:30:00 | INFO | Processing task for user. | message_id=12345 topic_name=tasks user_id=123
2025-10-25 13:30:00 | WARNING | User processing had a minor issue. | message_id=12345 topic_name=tasks user_id=123
2025-10-25 13:30:01 | INFO | This log will NOT have the user_id tag. | message_id=12345 topic_name=tasks
```

---

### Structured JSON Logging

For production, switch to structured JSON output for log aggregation platforms:

```bash
# Via environment variable
export FASTPUBSUB_ENABLE_LOG_SERIALIZE=1

# Or via CLI flag
fastpubsub run app:app --log-serialize
```

### Step-by-Step

1. Enable JSON logging with `FASTPUBSUB_ENABLE_LOG_SERIALIZE=1` or `--log-serialize`.
2. Run the app and confirm logs are JSON objects.
3. Filter by fields like `message_id`, `topic_name`, or your custom keys.

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

### Log Levels

You can define the appropriate log levels using the built-in `fastpubsub` CLI:

```python
--8<-- "observability/e1_01_logging_basics.py:log_levels"
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
--8<-- "observability/e1_04_cloud_logging.py"
```


---

## Recap

- **Built-in logger**: Import from `fastpubsub.logger` and use immediately
- **Context-aware**: Automatic `message_id`, `topic_name`, and handler context
- **Custom context**: Use `logger.contextualize()` for additional tags
- **JSON logs**: Enable with `--log-serialize` for production
- **Health checks**: `/consumers/alive` and `/consumers/ready` for orchestration
- **Log levels**: Use appropriate levels (debug, info, warning, error, critical)
