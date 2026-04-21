---
icon: lucide/terminal
---

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
--8<-- "basic_usage/e8_01_cli_example.py"
```

**Run with default settings:**

```bash
fastpubsub run my_project.main:app
```

---

## Step-by-Step

1. Set credentials or emulator host.
2. Point to your app: `module.path:app`.
3. Run `fastpubsub run`.
4. Check logs for subscriber startup.

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

### Wildcard Patterns

You can use glob patterns to select subscribers by alias prefix, suffix, or hierarchy:

```bash
# All subscribers under the "orders" prefix
fastpubsub run my_project.main:app -s 'orders.*'

# All "process" subscribers across any prefix
fastpubsub run my_project.main:app -s '*.process'

# All subscribers at any depth under "orders"
fastpubsub run my_project.main:app -s 'orders.**'

# "process" subscribers at any depth
fastpubsub run my_project.main:app -s '**.process'

# Compose patterns: ** and * together
fastpubsub run my_project.main:app -s '**.orders.*.process'

# Mix exact names and patterns
fastpubsub run my_project.main:app -s 'orders.*' -s payments.refund
```

Given the following application:

```python
--8<-- "basic_usage/e8_03_cli_wildcards.py:wildcard_subscribers"
```

The registered aliases are: `orders.process`, `orders.validate`, `orders.notify`, `payments.process`.

| Pattern | Matches |
|---------|---------|
| `orders.*` | `orders.process`, `orders.validate`, `orders.notify` |
| `*.process` | `orders.process`, `payments.process` |
| `orders.**` | `orders.process`, `orders.validate`, `orders.notify` |
| `**.process` | `orders.process`, `payments.process` |
| `order?.*` | `orders.process`, `orders.validate`, `orders.notify` |

**Supported wildcards:**

| Wildcard | Meaning |
|----------|---------|
| `*` | Matches any characters within a single segment (between dots) |
| `?` | Matches exactly one character |
| `**` | Matches zero or more dot-separated segments |

!!! note
    If a pattern does not match any subscriber, a warning is logged and the application continues. The application only fails to start if **no subscribers at all** are selected.

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

## Future Development

The framework is actively developed with planned features:

- **Full Support for Uvicorn**: At the moment we only support a few set of basic configurations for uvicorn. We intend to support them all in the future.
- **Publish Messages**: Publish messages locally to the configured emulator or directly sending to your handler.
- **Inspecting Components**: See [Inspecting Your Application](inspect.md) for listing subscribers, topics, and more.

---

## Recap

- **Core command**: `fastpubsub run module:app` is the main entry point
- **Development**: Use `--reload` for efficient local development
- **Production**: Use `--workers N` to scale across CPU cores
- **Granular control**: Use `--subscribers` to run specific handlers
- **Logging**: Configure with `--log-level` and `--log-serialize`
- **Flexibility**: Options can be set via CLI flags or environment variables
