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
--8<-- "basic_usage/e2_01_basic_publisher.py"
```

### When to Use

- Publishing to many different topics from the same function.
- Simple or infrequent publishing needs.
- Debugging or quick scripts.
- Topic name determined at runtime.

### Trade-offs
- Flexible and simple, but can become repetitive if you frequently publish to the same topic.
- The topic is specified every time, which can lead to typos caught only at runtime.
- Dependency injection becomes harder.

---

## Step-by-Step

1. Create a broker and app.
2. Decide on a topic and message schema.
3. Publish using `await broker.publish(...)`.
4. Confirm delivery by checking subscriber logs.

---

## Using Dedicated Publisher Objects

The approach involves `Publisher` object that is pre-configured for a specific topic. This is the ideal pattern when a part of your application is dedicated to publishing messages to a single topic, as it leads to cleaner, more maintainable, and testable code.


```python
--8<-- "basic_usage/e2_02_basic_publisher.py:publisher_instance"

--8<-- "basic_usage/e2_02_basic_publisher.py:publisher_instance_publish"
```

### Example with Dependency Injection

This pattern works well with clean architecture and dependency injection:

```python
--8<-- "basic_usage/e2_05_publisher_dependency_injection.py"
```

### When to Use

- A part of your application is dedicated to a single topic
- You want readable, reusable code (`user_publisher.publish(...)`)
- Using dependency injection
- Unit testing (easily mock the publisher)


### Trade-offs

- It requires a minor, one-time setup for each dedicated topic.
- This might feel like boilerplate if you have dozens of topics being published from a single module.


---

## Other Common Usages

Google Pub/Sub has features that let you control how data is delivered to the consumer. The next sections describe common configurations you will use when working with FastPubSub.


### Publishing with Attributes

Sometimes you need to add metadata to give context to your message events without modifying your schema. This is useful for server-side filtering or routing. In FastPubSub, you can add information to messages using their attributes. These map directly to Pub/Sub message attributes rather than the payload.

=== "Via `broker.publish` function"

    ```python hl_lines="6"
    --8<-- "basic_usage/e2_06_publish_with_attributes.py:publish_attributes_broker"
    ```

=== "Via `Publisher` object"

    ```python hl_lines="7"
    --8<-- "basic_usage/e2_06_publish_with_attributes.py:publish_attributes_publisher"
    ```

---

### Publishing with Ordering

For ordered message delivery, enable the `enable_message_ordering` on the receiving subscriber and provide an ordering key while publishing the message. FastPubSub's internal engine will handle all the Publisher configuration required to enable message ordering on Google's SDK. With that the messages with the same ordering key will be delivered in the order they were published.


=== "Via `broker.publish` function"

    ```python hl_lines="6 12"
    --8<-- "basic_usage/e2_07_publish_with_ordering.py:publish_ordering_broker"
    ```

=== "Via `Publisher` object"

    ```python hl_lines="9 14"
    --8<-- "basic_usage/e2_07_publish_with_ordering.py:publish_ordering_publisher"
    ```

---

### Cross-Project Publishing

In some scenarios, you may need to publish messages into projects that are not directly linked to the subscribers you created. FastPubSub allows you to publish to a topic in a different GCP project by overriding the `project_id` attribute.


=== "Via `broker.publish` function"

    ```python hl_lines="6"
    --8<-- "basic_usage/e2_08_cross_project_publish.py:cross_project_broker"
    ```

=== "Via `Publisher` object"

    ```python hl_lines="8"
    --8<-- "basic_usage/e2_08_cross_project_publish.py:cross_project_publisher"
    ```

---

## Recap

- **Two publishing patterns**: Direct `broker.publish()` for flexibility, dedicated `Publisher` objects for cleaner code.
- **Automatic serialization**: Pydantic models and dicts become JSON then bytes, strings become UTF-8, bytes sent as-is.
- **Attributes**: Add metadata for filtering and routing.
- **Ordering**: Enable ordering and use ordering keys for sequential delivery.
- **Cross-project**: Publish to topics in different GCP projects.
- **Always async**: All publishing must be awaited.
