---
icon: lucide/server
---

# Production Deployment Guide

FastPubSub applications are designed to be simple to run and scale. The `fastpubsub run` command uses Uvicorn under the hood and is production-capable when configured correctly.

```bash
fastpubsub run my_app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

!!! note

    FastPubSub integrates its consumer lifecycle directly into the CLI. You must use `fastpubsub run` to start your application. Running with Gunicorn or the `uvicorn` CLI directly is not supported.

---

## Deployment Concepts

### Replication

Scale by running multiple instances of the same `fastpubsub run` command. All instances connect to the same Pub/Sub subscription, and Google Cloud automatically load-balances messages between them.

### Statelessness

Design your consumers to be stateless. Message processing state should be managed by Pub/Sub (acknowledgments) or an external database. Stateless applications can be:

- Shut down, restarted, or moved without data loss.
- Automatically recovered by your orchestrator.
- Scaled based on demand.

### Multiple Workers

When you use `--workers N`, the CLI starts one master process managing N worker processes. Each worker:

- Is a separate Python process with its own memory.
- Loads your entire application independently.
- Bypasses the Python GIL for true parallel CPU utilization.


!!! warning "Resource Scaling"

    Be careful when increasing the number of workers. If one worker uses 100MB RAM, 4 workers use ~400MB total. 

---

## Deployment Checklist

1. Start with `fastpubsub run` (CLI-only).
2. Set `GOOGLE_APPLICATION_CREDENTIALS` or `PUBSUB_EMULATOR_HOST`.
3. Choose workers based on CPU and workload type.
4. Configure health probes (`/consumers/alive`, `/consumers/ready`).
5. Set `shutdown_timeout` and `terminationGracePeriodSeconds`.

---

## Kubernetes Deployment

### Strategy: 1 Worker per Pod

For container-based orchestration, use **1 worker per container** and let Kubernetes handle scaling:

```bash
# In your Dockerfile CMD
fastpubsub run app:app --host 0.0.0.0 --port 8000 --workers 1
```

Scale by increasing `replicas` in your Deployment. This provides:

- Finer-grained scaling (one Pod at a time).
- Better resource management.
- Isolation (one crash only affects one Pod).

### Dockerfile

Use a multi-stage build for a minimal production image:

```dockerfile
# --- Build Stage ---
FROM python:3.12 as builder

WORKDIR /usr/src/app

RUN pip install --upgrade pip && pip install poetry

COPY poetry.lock pyproject.toml ./

RUN poetry config virtualenvs.in-project true && \
    poetry install --no-root --no-dev --no-interaction --no-ansi


# --- Production Stage ---
FROM python:3.12-slim

WORKDIR /app

RUN groupadd -r appuser && useradd -r -g appuser appuser
USER appuser

COPY --from=builder --chown=appuser:appuser /usr/src/app/.venv ./.venv
COPY --chown=appuser:appuser . .

CMD [".venv/bin/fastpubsub", "run", "my_project.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
```

### Deployment Manifest

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fastpubsub-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fastpubsub-worker
  template:
    metadata:
      labels:
        app: fastpubsub-worker
    spec:
      containers:
      - name: app
        image: my-registry/my-fastpubsub-app:latest
        ports:
        - containerPort: 8000
        env:
        - name: "GCP_PROJECT_ID"
          value: "your-project-id"
        - name: "GOOGLE_APPLICATION_CREDENTIALS"
          value: "/path/to/creds/in/container"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "1"
        livenessProbe:
          httpGet:
            path: /consumers/alive
            port: 8000
          initialDelaySeconds: 15
          periodSeconds: 20
        readinessProbe:
          httpGet:
            path: /consumers/ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
      terminationGracePeriodSeconds: 60
```

### Health Probes Explained

| Probe | Endpoint | Checks |
|-------|----------|--------|
| **Liveness** | `/consumers/alive` | Web server is running, StreamingPull requests are active |
| **Readiness** | `/consumers/ready` | Broker has started all consumer tasks, actively polling |

If readiness returns `503`, the app is starting or has an error. Kubernetes waits before routing traffic.

### Graceful Shutdown

When Kubernetes sends `SIGTERM`, FastPubSub:

1. Signals all consumer tasks to stop polling
2. Waits for in-flight messages to be cancelled
3. Exits cleanly

Set `terminationGracePeriodSeconds` higher than your longest message processing time:

```yaml
terminationGracePeriodSeconds: 60  # Wait up to 60s
```

---

## Virtual Machine Deployment

### Multiple Workers

On a single VM, scale with the `--workers` flag:

```bash
fastpubsub run my_app.main:app --host 0.0.0.0 --port 8000 --workers 9
```

**Recommendation:** `(2 * CPU_CORES) + 1` workers. For a 4-core VM: `(2 * 4) + 1 = 9`.

### systemd Service

Run as a persistent service with systemd:

```ini
# /etc/systemd/system/fastpubsub.service
[Unit]
Description=FastPubSub Application
After=network.target

[Service]
Type=simple
User=appuser
Group=appuser
WorkingDirectory=/opt/my-app
Environment="GCP_PROJECT_ID=your-project-id"
Environment="GOOGLE_APPLICATION_CREDENTIALS=/opt/my-app/credentials.json"
ExecStart=/opt/my-app/.venv/bin/fastpubsub run my_project.main:app --host 0.0.0.0 --port 8000 --workers 4
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Commands:**

```bash
sudo systemctl enable fastpubsub
sudo systemctl start fastpubsub
sudo systemctl status fastpubsub
```

---

## Configuration Best Practices

### Load from Environment

Never hardcode production values:

```python
--8<-- "basic_usage/e8_02_deployment_config.py"
```

In Kubernetes, inject via `ConfigMaps` or `Secrets`.

### Logging to stdout

FastPubSub logs to `stdout` and `stderr` by default. In Kubernetes, container runtime captures these streams, and log aggregators (fluentd, Datadog) forward them to your logging solution.

Enable JSON logging for production:

```bash
fastpubsub run app:app --log-serialize
```

### Shutdown Timeout

Configure adequate shutdown time for in-flight messages:

```python
broker = PubSubBroker(
    project_id=PROJECT_ID,
    shutdown_timeout=30.0,  # Wait 30s for in-flight messages
)
```

---

## Hybrid vs. Standalone Apps

| Type | Description | Health Checks |
|------|-------------|---------------|
| **Hybrid** | Has both `@broker.subscriber` handlers and FastAPI endpoints | Built-in subscriber endpoints + your API endpoints |
| **Standalone** | Only `@broker.subscriber` handlers | Built-in subscriber endpoints only |

Both types use the same deployment strategy.

---

## Common Pitfalls

- Running with the `uvicorn` CLI directly (not supported).
- Setting too many workers for available memory.
- Using a shutdown timeout lower than typical processing time.

---

## Recap

- **Entrypoint**: Always use `fastpubsub run` to start your application
- **Configuration**: Load sensitive values from environment variables
- **Logging**: Log to stdout/stderr, use `--log-serialize` for JSON
- **Kubernetes**:
    - Use `--workers 1` in your container
    - Scale by increasing `replicas`
    - Use health check endpoints for probes
    - Set appropriate `terminationGracePeriodSeconds`
- **Virtual Machine**:
    - Use `--workers N` based on CPU cores
    - Manage with systemd as a long-running service
