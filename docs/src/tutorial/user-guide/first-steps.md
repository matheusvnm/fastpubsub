

# First Steps

This guide walks you through creating your first FastPubSub application. You'll build a simple app that subscribes to a topic, processes messages, and publishes new messages.

## Installation

Install FastPubSub with pip:

```bash 
pip install fastpubsub
```

--- 

## Core Concepts

FastPubSub has two main classes that form the backbone of every application:

| Class | Description |
|-------|-------------|
| `FastPubSub` | This is your application class with the logic to integrate with Pub/Sub and FastAPI. |
| `PubSubBroker` | Manages connections with Google Pub/Sub and handles subscribers and publishers. |

All Pub/Sub configuration attaches to the broker. The `FastPubSub` object takes a `PubSubBroker` instance as an argument. This separation lets you use all FastAPI features (middlewares, lifespan) with the application while integrating with the broker.

```python
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)
```

---

## Your First Application

Create a file named `basic.py`:

```python
from pydantic import BaseModel, Field
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.logger import logger


class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])


broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


@app.post("/addresses/")
async def create_address(address: Address):
    logger.info(f"Address received: {address}")
    await broker.publish(topic_name="address-events", data=address)
    return {"message": "Address published"}


@broker.subscriber(
    alias="address-handler",
    topic_name="address-events",
    subscription_name="address-events-subscription",
)
async def handle_message(message: Message):
    logger.info(f"The message {message.id} arrived.")
    address = Address.model_validate_json(message.data)
    logger.info(f"Address: {address}")
```

This application:

1. Defines a Pydantic model for validation
2. Creates a REST endpoint that publishes messages to a topic
3. Defines a subscriber that processes messages from that topic

---

## Set Up the Emulator

For local development, use the Google Pub/Sub emulator. Create a `docker-compose.yaml` file:

```yaml
services:
  pubsub:
    image: google/cloud-sdk:emulators
    command: gcloud beta emulators pubsub start --project fastpubsub-local --host-port 0.0.0.0:8085
    environment:
      - CLOUDSDK_CORE_PROJECT=fastpubsub-local
    volumes:
      - pubsub_data:/data
    ports:
      - "8085:8085"
    extra_hosts:
      - "localhost:host-gateway"

volumes:
  pubsub_data:
```

Start the emulator:

```bash
docker compose up -d pubsub
```

Set the environment variable to tell FastPubSub to use the emulator:

```bash
export PUBSUB_EMULATOR_HOST="localhost:8085"
```

---

## Run the Application

Use the built-in FastPubSub CLI:

```bash
fastpubsub run basic:app
```

For development with auto-reload:

```bash
fastpubsub run basic:app --reload
```

---

## Test It

Send a POST request to the `/addresses/` endpoint:

```bash
curl -X POST "http://127.0.0.1:8000/addresses/" \
  -H "Content-Type: application/json" \
  -d '{"street": "5th Avenue", "number": "1548"}'
```

You should see output like this in your terminal:

```
2025-10-17 11:37:30,363 | INFO | runner:run:55 | FastPubSub app starting...
2025-10-17 11:37:30,650 | INFO | tasks:start:74 | The handle_message handler is waiting for messages.
2025-10-17 11:37:33,791 | INFO | basic:create_address:15 | Address received: street='5th Avenue' number='1548'
2025-10-17 11:37:33,821 | INFO | pubsub:publish:305 | Message published for topic projects/your-project-id/topics/address-events with id 1
2025-10-17 11:37:33,832 | INFO | basic:handle_message:25 | The message 1 arrived. | name=address-handler message_id=1 topic_name=address-events
2025-10-17 11:37:33,832 | INFO | basic:handle_message:27 | Address: street='5th Avenue' number='1548' | name=address-handler message_id=1 topic_name=address-events
2025-10-17 11:37:33,851 | INFO | tasks:_consume:131 | Message successfully processed. | name=address-handler message_id=1 topic_name=address-events
```

Notice how the logs include context like `message_id`, `topic_name`, and the handler `name`. FastPubSub automatically adds this information to help with debugging and monitoring.

---

## Understanding the Code

### The Broker

```python
broker = PubSubBroker(project_id="your-project-id")
```

The broker manages all Pub/Sub connections. It handles:

- Creating topics and subscriptions (when `autocreate=True`)
- Managing message acknowledgments
- Coordinating publishers and subscribers

### The Application

```python
app = FastPubSub(broker)
```

The application is a FastAPI instance with Pub/Sub integration. You can use all FastAPI features like:

- Path operations (`@app.get()`, `@app.post()`)
- Dependency injection
- Middleware
- OpenAPI documentation

### The Subscriber

```python
@broker.subscriber(
    alias="address-handler",
    topic_name="address-events",
    subscription_name="address-events-subscription",
)
async def handle_message(message: Message):
    ...
```

The `@broker.subscriber` decorator registers an async function as a message handler. Key parameters:

| Parameter | Description |
|-----------|-------------|
| `alias` | A unique name for this subscriber (used in CLI and logs) |
| `topic_name` | The Pub/Sub topic to subscribe to |
| `subscription_name` | The Pub/Sub subscription name |

By default, `autocreate=True`, so FastPubSub creates the topic and subscription if they don't exist.

### Publishing

```python
await broker.publish(topic_name="address-events", data=address)
```

The broker's `publish` method sends messages to a topic. It automatically serializes:

- Pydantic models to JSON
- Dictionaries to JSON
- Strings to UTF-8 bytes
- Bytes are sent as-is

---

## More Examples

For additional examples and patterns, check the [examples directory](https://github.com/matheusvnm/fastpubsub/tree/master/examples) in the repository:

- **Basic Usage**: Simple subscribers, publishers, and cross-project communication
- **Routers**: Organizing subscribers with prefixes and nested hierarchies
- **Middlewares**: Custom middleware implementations and hierarchy patterns

---

## Recap

In this guide, you learned:

- **Core classes**: `FastPubSub` and `PubSubBroker` and their roles
- **Creating an application**: Combining REST endpoints with Pub/Sub subscribers
- **Local development**: Using the Pub/Sub emulator with Docker
- **Running the app**: Using the `fastpubsub run` CLI command
- **Automatic logging**: FastPubSub adds context to logs for debugging

