---
icon: lucide/activity
---

# Lifespan and Hooks

FastPubSub manages the application lifecycle using a built-in lifespan event handler. This implementation is based on the standard [FastAPI lifespan context manager](https://fastapi.tiangolo.com/advanced/events/) and runs the broker and subscribers within the same event loop as the web server.

When your FastPubSub application starts, the internal lifespan function handles starting and stopping the broker. It provides four hook decorators for adding custom logic at specific moments.

## The Four Event Hooks

### `@app.on_startup`

Runs after the application process starts but **before** `broker.start()` is called.

**Use it for:** Setting up essential resources that subscribers need before they start pulling messages.

```python hl_lines="7"
import asyncpg
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.on_startup
async def setup_database():
    print("Connecting to the database...")
    app.state.db_pool = await asyncpg.create_pool(
        user="user", password="password", database="db", host="127.0.0.1"
    )
    print("Database connection pool created.")
```

### `@app.after_startup`

Runs immediately **after** `broker.start()` completes successfully. Subscribers are now running and polling for messages.

**Use it for:** Logic that needs to interact with the active broker or subscribers.

```python hl_lines="6"
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.after_startup
async def announce_startup():
    print("Subscribers are running. Publishing startup message.")
    await broker.publish("system-logs", data={"status": "online"})
```

### `@app.on_shutdown`

Runs when the application receives a shutdown signal (e.g., `SIGTERM`), but **before** `broker.shutdown()` is called. Subscribers are still running.

**Use it for:** Initiating graceful shutdown. Set a flag to tell long-running handlers to finish up.

```python hl_lines="6"

from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.on_shutdown
async def prepare_for_shutdown():
    print("Shutdown signal received. Preparing to stop...")
    app.state.is_shutting_down = True
```

### `@app.after_shutdown`

Runs **after** `broker.shutdown()` completes and all subscriber tasks have stopped. This is the last FastPubSub code to execute.

**Use it for:** Final cleanup and releasing resources created in `on_startup`.

```python hl_lines="6"
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.after_shutdown
async def cleanup_database():
    if hasattr(app.state, "db_pool"):
        print("Closing database connection pool...")
        await app.state.db_pool.close()
        print("Database pool closed.")
```

---

## Execution Order

The lifecycle follows a strict order. The `on_*` hooks run before the broker action, and `after_*` hooks run after.

```mermaid
sequenceDiagram
    participant CLI
    participant App
    participant Broker

    CLI->>App: fastpubsub run ...

    Note over App: Enters lifespan (before yield)
    App->>App: Executes @app.on_startup hooks
    App->>Broker: broker.start()
    Broker-->>App: Subscribers are running
    App->>App: Executes @app.after_startup hooks
    Note over App, Broker: Application is running...

    CLI->>App: (Shutdown signal received)

    Note over App: Resumes lifespan (after yield)
    App->>App: Executes @app.on_shutdown hooks
    App->>Broker: broker.shutdown()
    Broker-->>App: Subscribers are stopped
    App->>App: Executes @app.after_shutdown hooks
    App-->>CLI: Process exits
```

---

## Custom Lifespan

For advanced use cases, pass a custom lifespan context manager to FastPubSub. The built-in lifecycle (including all four hooks and broker start/stop) executes within the `yield` block of your custom function.

```python hl_lines="5 20 30 34 43"
from contextlib import asynccontextmanager
import httpx
from fastpubsub import FastPubSub, PubSubBroker

@asynccontextmanager
async def global_lifespan(app: FastPubSub):
    print("GLOBAL LIFESPAN: Starting up...")
    # Create a shared HTTP client
    async with httpx.AsyncClient() as client:
        app.state.http_client = client
        print("GLOBAL LIFESPAN: HTTP client created.")
        yield
    print("GLOBAL LIFESPAN: HTTP client closed.")
    print("GLOBAL LIFESPAN: Shutting down...")

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker, lifespan=global_lifespan)


@app.on_startup
async def on_startup_hook():
    # Runs after http_client is created
    print("  FastPubSub Hook: @app.on_startup")
    try:
        await app.state.http_client.post("http://service-registry/register")
        print("  FastPubSub Hook: Registered with service registry.")
    except Exception as e:
        print(f"  FastPubSub Hook: Failed to register: {e}")

@app.after_startup
async def after_startup_hook():
    print("  FastPubSub Hook: @app.after_startup (Broker is running)")

@app.on_shutdown
async def on_shutdown_hook():
    print("  FastPubSub Hook: @app.on_shutdown")
    try:
        await app.state.http_client.post("http://service-registry/deregister")
        print("  FastPubSub Hook: Deregistered from service registry.")
    except Exception as e:
        print(f"  FastPubSub Hook: Failed to deregister: {e}")

@app.after_shutdown
async def after_shutdown_hook():
    print("  FastPubSub Hook: @app.after_shutdown (Broker is stopped)")
```

This separates global resources (HTTP client) from broker-specific logic.

---

## Common Patterns

### Database Connection Pool

```python
import asyncpg
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.on_startup
async def create_pool():
    app.state.pool = await asyncpg.create_pool(
        "postgresql://user:pass@localhost/db"
    )

@app.after_shutdown
async def close_pool():
    await app.state.pool.close()

@broker.subscriber(
    alias="db-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def save_event(message: Message):
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO events (data) VALUES ($1)",
            message.data.decode("utf-8")
        )
```

### HTTP Client Session

```python
import httpx
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

@app.on_startup
async def create_client():
    app.state.http = httpx.AsyncClient(timeout=30.0)

@app.after_shutdown
async def close_client():
    await app.state.http.aclose()

@broker.subscriber(
    alias="webhook-handler",
    topic_name="notifications",
    subscription_name="notifications-subscription",
)
async def send_webhook(message: Message):
    await app.state.http.post(
        "https://api.example.com/webhook",
        json={"data": message.data.decode("utf-8")}
    )
```

### Graceful Shutdown Flag

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker("your-project-id")
app = FastPubSub(broker)

app.state.shutting_down = False

@app.on_shutdown
async def set_shutdown_flag():
    app.state.shutting_down = True

@broker.subscriber(
    alias="long-task-handler",
    topic_name="long-tasks",
    subscription_name="long-tasks-subscription",
)
async def handle_long_task(message: Message):
    for step in range(10):
        if app.state.shutting_down:
            # Finish gracefully instead of starting new work
            break
        await process_step(step, message.data)
```

---

## Recap

- **Built on FastAPI's Lifespan**: FastPubSub uses a built-in function that follows the standard lifespan context manager pattern
- **Four hooks for precise control**:
    - `on_startup`: Before broker starts
    - `after_startup`: After broker starts
    - `on_shutdown`: Before broker stops
    - `after_shutdown`: After broker stops
- **Custom lifespan support**: Pass your own lifespan function for advanced resource management
- **Common patterns**: Database pools, HTTP clients, and graceful shutdown flags
