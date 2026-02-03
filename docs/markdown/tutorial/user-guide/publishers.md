---
icon: lucide/send
---

# Publishers

A **Publisher** sends messages to a Google Cloud Pub/Sub topic in a set project. FastPubSub provides two patterns for publishing:

1. A central broker method for flexibility.

2. A dedicated publisher object for cleaner code.

### Core Responsibilities

The publisher handles:

- **Connection Management**: Efficiently manages gRPC connections, opening and closing them appropriately to reduce resource consumption.

- **Asynchronous Operations**: All publishing calls are async, allowing the event loop to handle other tasks while waiting.

- **Automatic Serialization**: Converts Python data into byte strings that Pub/Sub requires.

### Serialization Strategy

The publisher automatically converts your data:

| Type | Serialization |
|------|---------------|
| Pydantic `BaseModel` | JSON bytes (`{"key": "value"}` → `b'{"key":"value"}'`) |
| `dict` | JSON bytes  (`{"key": "value"}` → `b'{"key":"value"}'`) |
| `str` | UTF-8 bytes (`"hello"` → `b'hello'`) |
| `bytes` | Sent as-is (for custom formats like Protobuf or Avro) |

---

## Publishing with the Broker

The most direct and flexible way to publish a message. You call the `broker.publish()` method on your central broker object, specifying the destination topic for each call.

```python
await broker.publish(topic_name="my-topic", data={"hello": "world"})
```

### Example

```python
from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="test-handler",
    topic_name="test-topic",
    subscription_name="test-subscription",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")

@app.after_startup
async def publish_on_startup() -> None:
    logger.info("Publishing a message directly via the broker...")
    await broker.publish("test-topic", {"hello": "world"})
```

### When to Use

- Publishing to many different topics from the same function.
- Simple or infrequent publishing needs.
- Debugging or quick scripts.
- Topic name determined at runtime.

Flexible and simple, but can become repetitive if you frequently publish to the same topic. The topic is specified every time, which can lead to typos caught only at runtime.

---

## Using Dedicated Publisher Objects

The approach involves `Publisher` object that is pre-configured for a specific topic. This is the ideal pattern when a part of your application is dedicated to publishing messages to a single topic, as it leads to cleaner, more maintainable, and testable code.


```python
# Create once
user_events_publisher = broker.publisher("user-events-topic")

# Use anywhere without specifying topic
await user_events_publisher.publish(data={"event": "login"})
```

### Example with Dependency Injection

This pattern works well with clean architecture and dependency injection:

```python
from typing import Any
from dataclasses import dataclass
from pydantic import BaseModel
from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger


@dataclass
class MyAwesomeUseCase:
    publisher: Publisher

    async def execute(self, data: dict) -> Any:
        # Business logic here...
        # Then publish the event
        return await self.publisher.publish(data=data)


class User(BaseModel):
    name: str
    age: int


broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

# Create a dedicated publisher for user events
user_publisher = broker.publisher("new-users-topic")


@app.post("/new-user")
async def receive_new_user(user: User) -> dict[str, str]:
    logger.info(f"Received a new user: {user.name}")

    # Inject the dedicated publisher into the use case
    # Easy to mock in tests
    use_case = MyAwesomeUseCase(publisher=user_publisher)
    await use_case.execute(user.model_dump())

    return {"message": "Use case executed successfully"}
```

### When to Use

- A part of your application is dedicated to a single topic
- You want readable, reusable code (`user_publisher.publish(...)`)
- Using dependency injection
- Unit testing (easily mock the publisher)

It requires a minor, one-time setup for each dedicated topic. This might feel like boilerplate if you have dozens of topics being published from a single module, in which case the direct broker method might be more appropriate for that specific scenario.

---

## Other Common Usages

Google PubSub has some great feature that allow the developer some flexibility of how the data is delivered to the consumer. The next sections describe some of the common configurations you will use the most when working with FastPubSub.


### Publishing with Attributes

Sometimes you need to add metadata for adding context to your message events without modifing your schema. Such scenarios may arise when you need server-side filtering or adding information for routing. On FastPubSub, you can add information to messages using their attributes. These will be directly linked to the PubSub message attributes rather then its payload.

=== "Via `broker.publish` function"

    ```python hl_lines="4"
    await broker.publish(
        topic_name="events",
        data={"user_id": "123", "action": "login"},
        attributes={"event_type": "user_login", "priority": "high"}
    )
    ```

=== "Via `Publisher` object"

    ```python hl_lines="5"
    event_publisher = broker.publisher("events")

    await event_publisher.publish(
        data={"user_id": "123", "action": "login"},
        attributes={"event_type": "user_login", "priority": "high"}
    )
    ```

---

### Publishing with Ordering

For ordered message delivery, enable the `enable_message_ordering` on the receiving subscriber and provide an ordering key while publishing the message. FastPubSub's internal engine will handle all the Publisher configuration required to enable message ordering on Google's SDK. With that the messages with the same ordering key will be delivered in the order they were published.


=== "Via `broker.publish` function"

    ```python hl_lines="4 10"

    await broker.publish(
        topic_name="user-events",
        data={"action": "login", "user_id": "user-123"},
        ordering_key="user-123" # Same key ensures order
    )

    await broker.publish(
        topic_name="user-events",
        data={"action": "update_profile", "user_id": "user-123"},
        ordering_key="user-123"  # Same key ensures order
    )
    ```


=== "Via `Publisher` object"

    ```python hl_lines="6 11"

    ordered_publisher = broker.publisher("user-events")

    # Publish with ordering key
    await ordered_publisher.publish(
        data={"action": "login", "user_id": "user-123"},
        ordering_key="user-123" # Same key ensures order
    )

    await ordered_publisher.publish(
        data={"action": "update_profile", "user_id": "user-123"},
        ordering_key="user-123"  # Same key ensures order
    )
    ```



---

### Cross-Project Publishing

On some scenarios, you may need to publish messages into projects that is not directly linked to the subscribers you created. FastPubSub allows you to publish to a topic in different GCP project id by just overriding the `project_id` attribute.


=== "Via `broker.publish` function"

    ```python hl_lines="4"
    await broker.publish(
        topic_name="shared-events",
        data={"event": "cross_project"},
        project_id="other-project-id"
    )
    ```


=== "Via `Publisher` object"

    ```python hl_lines="3"
    cross_project_publisher = broker.publisher(
        "shared-events",
        project_id="other-project-id"
    )
    await cross_project_publisher.publish(data={"event": "cross_project"})
    ```


---

## Recap

- **Two publishing patterns**: Direct `broker.publish()` for flexibility, dedicated `Publisher` objects for cleaner code.
- **Automatic serialization**: Pydantic models and dicts become JSON then bytes, strings become UTF-8, bytes sent as-is.
- **Attributes**: Add metadata for filtering and routing.
- **Ordering**: Enable ordering and use ordering keys for sequential delivery.
- **Cross-project**: Publish to topics in different GCP projects.
- **Always async**: All publishing must be awaited.
