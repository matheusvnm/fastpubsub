---
icon: lucide/rocket
---

# First Steps

This guide walks you through creating your first FastPubSub application. You'll build a simple app that subscribes to a topic, processes messages, and publishes new messages.

## Installation

Install FastPubSub with pip:

```bash
pip install fastpubsub
```

---

## Prerequisites

You need one of the following before running the app:

- **Cloud Pub/Sub**: Set `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON file.
- **Emulator**: Set `PUBSUB_EMULATOR_HOST` to the emulator host/port.

---

## Core Concepts

FastPubSub has two main classes that form the backbone of every application:

| Class | Description |
|-------|-------------|
| `FastPubSub` | This is your application class with the logic to integrate with Pub/Sub and FastAPI. |
| `PubSubBroker` | Manages connections with Google Pub/Sub and handles subscribers and publishers. |

All Pub/Sub configuration attaches to the broker. The `FastPubSub` object takes a `PubSubBroker` instance as an argument. This separation lets you use all FastAPI features (middlewares, lifespan) with the application while integrating with the broker.

```python
--8<-- "basic_usage/e0_01_first_steps.py:broker_app"
```

---

## Your First Application

Create a file named `basic.py`:

```python
--8<-- "basic_usage/e0_01_first_steps.py"
```

This application:

1. Defines a Pydantic model for validation
2. Creates a REST endpoint that publishes messages to a topic
3. Defines a subscriber that processes messages from that topic

---

## Step-by-Step

1. Create the broker and app.
2. Define your message model.
3. Add an API endpoint that publishes.
4. Add a subscriber that consumes.
5. Run the CLI and send a test request.

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
2026-02-04 21:14:08,423 | INFO     | 89994:8702897216 | runner:run:76 | FastPubSub app starting... 
2026-02-04 21:14:08,502 | INFO     | 89994:8702897216 | tasks:start:80 | The handle_message handler is waiting for messages. 
2026-02-04 21:14:15,585 | INFO     | 89994:8702897216 | e0_01_first_steps:create_address:22 | Address received: street='5th Avenue' number='1548' 
2026-02-04 21:14:15,618 | INFO     | 89994:8702897216 | pubsub:publish:248 | Message published for topic projects/your-project-id/topics/address-events with id 19 
2026-02-04 21:14:15,666 | INFO     | 89994:8702897216 | e0_01_first_steps:handle_message:35 | The message 19 arrived. | message_id=19 | topic_name=address-events | subscriber_name=handle_message 
2026-02-04 21:14:15,667 | INFO     | 89994:8702897216 | e0_01_first_steps:handle_message:37 | Address: street='5th Avenue' number='1548' | message_id=19 | topic_name=address-events | subscriber_name=handle_message 
2026-02-04 21:14:15,668 | INFO     | 89994:8702897216 | tasks:_consume:107 | The message successfully processed. | message_id=19 | topic_name=address-events | subscriber_name=handle_message 
```

Notice how the logs include context like `message_id`, `topic_name`, and the handler `subscriber_name`. FastPubSub automatically adds this information to help with debugging and monitoring.

---

## Understanding the Code

### The Broker

The broker manages all Pub/Sub connections. It handles:

- Creating topics and subscriptions (when `autocreate=True`)
- Managing message acknowledgments
- Coordinating publishers and subscribers

### The Application

The application is a FastAPI instance with Pub/Sub integration. You can use all FastAPI features like:

- Path operations (`@app.get()`, `@app.post()`)
- Dependency injection
- Middleware
- OpenAPI documentation

### The Subscriber

```python
--8<-- "basic_usage/e0_01_first_steps.py:subscriber"
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
--8<-- "basic_usage/e0_01_first_steps.py:rest_endpoint"
```

The broker's `publish` method sends messages to a topic. It automatically serializes:

- Pydantic, dictionaries, and strings to bytes.
- Bytes are sent as-is.

---

## Recap

In this guide, you learned:

- **Core classes**: `FastPubSub` and `PubSubBroker` and their roles
- **Creating an application**: Combining REST endpoints with Pub/Sub subscribers
- **Local development**: Using the Pub/Sub emulator with Docker
- **Running the app**: Using the `fastpubsub run` CLI command
- **Automatic logging**: FastPubSub adds context to logs for debugging
