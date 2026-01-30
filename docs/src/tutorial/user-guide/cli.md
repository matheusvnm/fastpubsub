# Command-Line Interface (CLI)

The `fastpubsub` CLI is a production-ready tool for running and managing your applications. Built with [Typer](https://github.com/fastapi/typer) and powered by [Uvicorn](https://github.com/Kludex/uvicorn), it provides a seamless experience for both local development and production deployments.

## How It Works

When you execute the `run` command, the CLI:

1. **Checks authentication**: Verifies Google Cloud credentials are set up (`GOOGLE_APPLICATION_CREDENTIALS` or `PUBSUB_EMULATOR_HOST`)
2. **Loads configuration**: Parses command-line arguments and sets environment variables
3. **Imports the application**: Loads your FastPubSub app using the specified path (e.g., `my_app.main:app`)
4. **Starts Uvicorn**: Hands over to Uvicorn, which runs your app. Subscribers start as background tasks within the event loop

---

## Prerequisites

Before running the CLI, authenticate with Google Cloud or start the emulator:

```bash
# For cloud environments
gcloud auth application-default login

# For local development with emulator
export PUBSUB_EMULATOR_HOST=localhost:8085
```

---

## Basic Usage

The primary command is `run`, which takes one required argument: the path to your FastPubSub application in the format `path.to.module:variable_name`.

**Example application (`my_project/main.py`):**

```python
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@broker.subscriber("process-orders", topic_name="orders", subscription_name="orders-sub")
async def handle_orders(message): ...

@broker.subscriber("send-notifications", topic_name="notifications", subscription_name="notifications-sub")
async def handle_notifications(message): ...
```

**Run with default settings:**

```bash
fastpubsub run my_project.main:app
```

---

## Development Mode

### Hot-Reloading

For local development, use `--reload` to automatically restart when you save a file:

```bash
fastpubsub run my_project.main:app --reload
```

!!! note

    When using `--reload`, the `--workers` option is ignored. The application runs in a single process.

---

## Running Specific Subscribers

In larger applications, you might want to run only a subset of subscribers. Use the `-s` or `--subscribers` flag:

```bash
# Run only one subscriber
fastpubsub run my_project.main:app -s process-orders

# Run multiple specific subscribers
fastpubsub run my_project.main:app -s process-orders -s send-notifications
```

This is useful for:

- Running different subscribers on different machines
- Testing specific handlers in isolation
- Scaling individual subscribers independently

---

## Production Mode

### Multiple Workers

For production, run multiple worker processes to utilize multiple CPU cores:

```bash
fastpubsub run my_project.main:app --workers 4
```

Each worker is a separate Python process with its own event loop, allowing true parallel execution.

!!! tip "Worker Count"

    A common recommendation is `(2 * CPU_CORES) + 1`. For a 4-core machine: `(2 * 4) + 1 = 9` workers.

### Host and Port

Configure the network binding:

```bash
fastpubsub run my_project.main:app --host 0.0.0.0 --port 8000
```

---

## Logging Options

Control log verbosity and format:

```bash
# Debug-level logging
fastpubsub run my_project.main:app --log-level debug

# Structured JSON logging for production
fastpubsub run my_project.main:app --log-serialize

# Combine options
fastpubsub run my_project.main:app --log-level info --log-serialize
```

Available log levels: `debug`, `info`, `warning`, `error`, `critical`

---

## CLI Options Reference

| Option | Description | Default |
|--------|-------------|---------|
| `--host` | Bind to this host | `127.0.0.1` |
| `--port` | Bind to this port | `8000` |
| `--workers` | Number of worker processes | `1` |
| `--reload` | Enable hot-reloading for development | `False` |
| `-s`, `--subscribers` | Run only these subscribers (repeatable) | All |
| `--log-level` | Application log level | `info` |
| `--log-serialize` | Output logs as JSON | `False` |
| `--server-log-level` | Uvicorn server log level | `info` |

---

## Uvicorn Integration

The `fastpubsub` CLI is a wrapper around Uvicorn. These options map directly to Uvicorn parameters:

| CLI Option | Uvicorn Parameter |
|------------|-------------------|
| `--host` | `host` |
| `--port` | `port` |
| `--workers` | `workers` |
| `--reload` | `reload` |
| `--server-log-level` | `log_level` |

This unified approach provides a single interface for configuring both web and messaging aspects.

---

## Environment Variables

Many options can be set via environment variables:

| Variable | Description |
|----------|-------------|
| `PUBSUB_EMULATOR_HOST` | Connect to emulator instead of cloud |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account key |
| `FASTPUBSUB_ENABLE_LOG_SERIALIZE` | Enable JSON logging (`1` or `true`) |

---

## Examples

### Local Development

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
fastpubsub run my_app.main:app --reload --log-level debug
```

### Production Single Machine

```bash
fastpubsub run my_app.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --log-serialize
```

### Running Specific Subscribers

```bash
# Machine 1: Order processing
fastpubsub run my_app.main:app -s order-handler --workers 4

# Machine 2: Notification sending
fastpubsub run my_app.main:app -s notification-handler --workers 2
```

---

## Recap

- **Core command**: `fastpubsub run module:app` is the main entry point
- **Development**: Use `--reload` for efficient local development
- **Production**: Use `--workers N` to scale across CPU cores
- **Granular control**: Use `--subscribers` to run specific handlers
- **Logging**: Configure with `--log-level` and `--log-serialize`
- **Flexibility**: Options can be set via CLI flags or environment variables
