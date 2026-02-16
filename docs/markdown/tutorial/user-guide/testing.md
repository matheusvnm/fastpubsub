---
icon: lucide/test-tube-2
---

# Testing

Testing is a critical part of building reliable event-driven applications.
When working with Google Cloud Pub/Sub, testing introduces unique challenges
that don't exist in traditional request-response systems.

## Why Testing Matters

In a Pub/Sub architecture, messages flow asynchronously between producers and
consumers. A bug in serialization, routing, or attribute handling may not
surface until runtime — often in production.

Traditional testing approaches come with significant friction:

- **Emulator setup**: Google provides a Pub/Sub emulator, but it requires
  Docker, port configuration, and environment variables
  (`PUBSUB_EMULATOR_HOST`). Also, the emulator does not provides all features of a full-fledged real Pub/Sub.
- **Infrastructure dependency**: Integration tests against real Pub/Sub
  require a GCP project, service accounts, network access and often generates infrastructure costs.
- **Controlled execution**: Pub/Sub is inherently asynchronous, making it
  hard to deterministically assert outcomes in tests.

Unit tests using `unittest.mock` can verify that your handler was called, but
they won't catch problems like incorrect topic routing, broken serialization,
or filter expression mismatches. These are integration-level concerns.

FastPubSub addresses this with `PubSubTestClient` — an in-memory test utility
that routes messages through your real handlers without any external
infrastructure.

The following section explain how to use the client on its basic structure.
If you need advanced testing patterns (e.g., filter expressions, middleware testing, cross-porject routing),
it will be covered in the advanced usage guide unde development.

---

## Async Tests

As you've seen throughout this guide, FastPubSub is built entirely on
`asyncio`. Subscribers, publishers, and lifecycle hooks are all async
functions.

Your tests need to follow the same async programming model because the
`PubSubTestClient` is used as an async context manager:

```python
async with PubSubTestClient(broker) as client:
    await client.publish("hello", topic="my-topic")
```

This is particularly valuable when your handlers interact with external
systems like databases or HTTP APIs — you can test the full async flow
end-to-end.

However, you will need additional libraries to run async test functions other than just `pytest`.

Here are the two most common options:


=== "pytest-asyncio"

    Install the plugin:

    ```shell
    pip install pytest-asyncio
    ```

    Then mark your test functions with `@pytest.mark.asyncio`:

    ```python
    --8<-- "testing/e1_01_setup_asyncio.py"
    ```

=== "anyio"

    Install the plugin:

    ```shell
    pip install anyio
    ```

    Then mark your test functions with `@pytest.mark.anyio`:

    ```python
    --8<-- "testing/e1_02_setup_anyio.py"
    ```

!!! note "Our examples"
    All examples in this guide use `pytest-asyncio`. Replace
    `@pytest.mark.asyncio` with `@pytest.mark.anyio` if you prefer anyio.


!!! tip "Pro-tip"
    FastPubSub ships with async-ready tools like
    [HTTPX](https://www.python-httpx.org/) for making async HTTP requests
    inside your handlers.

---

## Testing Published Messages

When your subscriber processes a message and publishes downstream, you need to
verify the right messages were sent to the right topics. `PubSubTestClient` records every
message published during the test session. You can inspect them with `get_published_messages()`.

### Step 1: Define Your Broker and Subscriber

Create a simple application (e.g., `test_published.py`) where a subscriber receives an order and publishes
a confirmation to another topic:

```python
--8<-- "testing/e1_04_test_published_messages.py:app"
```

### Step 2: Write the Test

Publish a test message and inspect what was sent downstream:

```python
--8<-- "testing/e1_04_test_published_messages.py:test"
```

Each element inside the array of elements returned from `get_published_messages()` is a `PublishedMessage` object. It has the
a series of fields you can assert to check if the message is correct (e.g., `project_id`, `topic_name`, `data`, `attributes`, etc):

!!! tip "All published messages"
    The array returned by `get_published_messages()` includes **all** messages
    published during the session — both the ones you sent via
    `client.publish()` and the ones your handlers sent by itself.

### Step 3: Run the Test

```shell
pytest tests/test_published.py -v
```

Expected output:

```shell
tests/test_published.py::test_message_is_forwarded PASSED
```

---

## Testing Processed Messages

Sometimes you need to verify that the message processing happened correctly.
For that the `PubSubTestClient` provides a way for checking return values, side effects, or errors.
You only need to call `get_results()` and it will return the information you need.

### Step 1: Define a Subscriber That Returns Data

Create an application (e.g., `test_processed.py`) where a subscriber processes orders and returns some information.
On our examples, we will just return a string but it can be any data:

```python
--8<-- "testing/e1_05_test_processed_messages.py:app"
```

### Step 2: Write the Test

Publish test messages and inspect how each one was processed:

```python
--8<-- "testing/e1_05_test_processed_messages.py:test"
```

Each element inside the array of elements returned from `get_results()` is a `ProcessingResult` object.
It provides you a series of fields you can assert to check if the message is processed
(e.g., `message`, `return_value` and `error`).


!!! note "Multiple subscribers"
    We store the result of each subscriber that received the message. If you have more than one subscriber attached to
    the topic you sent the message, `get_results()` will have a `ProcessingResult` for each of them.

### Step 3: Run the Test

```shell
pytest tests/test_processed.py -v
```

Expected output:

```shell
tests/test_processed.py::test_order_is_processed PASSED
tests/test_processed.py::test_invalid_order_raises_error PASSED
```

---

## Recap

In this chapter you learned:

- **Why testing Pub/Sub is hard**: Emulators, Docker, controlled async
  execution, and the limits of mock-based unit tests.
- **PubSubTestClient**: An in-memory test utility that routes messages
  through your real handlers with zero external infrastructure.
- **Async test setup**: How to configure `pytest-asyncio` or `anyio` to run
  async test functions.
- **Testing published messages**: Use `get_published_messages()` to inspect
  what your application sent downstream.
- **Testing processed messages**: Use `get_results()` to verify handler
  return values and errors.
