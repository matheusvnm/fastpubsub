---
icon: lucide/test-tube-2
---

# Testing

Testing is crucial for building reliable Pub/Sub applications. FastPubSub provides utilities for fast unit tests and supports integration testing with the emulator.

## Testing Strategies

FastPubSub applications can be tested at multiple levels:

| Level | Description | Speed |
|-------|-------------|-------|
| **Unit Tests** | Test handlers in isolation with `PubSubTestClient` | Fast |
| **Integration Tests** | Test full message flow with the emulator | Slower |
| **End-to-End Tests** | Test with real infrastructure | Slowest |

---

## Unit Testing with PubSubTestClient

`PubSubTestClient` is an in-memory testing utility. Test your message handlers without the emulator.

### Step-by-Step

1. Create a broker and register handlers.
2. Open a `PubSubTestClient` context.
3. Publish test messages.
4. Assert side effects or captured messages.

### Basic Example

```python
--8<-- "testing/e1_01_test_client.py:basic_test"
```

### Testing with Pydantic Models

```python
--8<-- "testing/e2_02_pydantic_testing.py"
```

### Testing Exception Handling

```python
--8<-- "testing/e2_03_exception_testing.py"
```

---

## Mocking

### Mocking Publishers

When testing code that publishes messages, mock the publisher to avoid actual publishing:

```python
--8<-- "testing/e2_04_mocking_publishers.py"
```

### Mocking External Dependencies

```python
import pytest
from unittest.mock import AsyncMock, patch
from fastpubsub import PubSubBroker, Message
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="test-project")

@broker.subscriber(
    alias="email-handler",
    topic_name="user-created",
    subscription_name="user-created-subscription",
)
async def send_welcome_email(message: Message):
    from my_app.email_service import EmailService
    email_service = EmailService()
    await email_service.send_email(message.data.decode("utf-8"))

@pytest.mark.asyncio
async def test_send_welcome_email():
    with patch('my_app.email_service.EmailService') as MockEmailService:
        mock_email = AsyncMock()
        MockEmailService.return_value = mock_email

        async with PubSubTestClient(broker) as client:
            await client.publish("user-created", data=b"user@example.com")

            mock_email.send_email.assert_called_once()
```

---

## Testing Routers

Routers are tested the same way as the main broker:

```python
--8<-- "testing/e2_05_testing_routers.py"
```

---

## Integration Testing with Emulator

For integration tests, use the Google Pub/Sub emulator to test the full message flow.

### Setting Up the Emulator

**docker-compose.yaml:**

```yaml
services:
  pubsub-emulator:
    image: google/cloud-sdk:emulators
    command: gcloud beta emulators pubsub start --project=test-project --host-port=0.0.0.0:8085
    ports:
      - "8085:8085"
```

**Start the emulator:**

```bash
docker compose up -d pubsub-emulator
export PUBSUB_EMULATOR_HOST="localhost:8085"
```

### Integration Test Example

```python
import asyncio
import os
import pytest
from fastpubsub import PubSubBroker, Message

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"

@pytest.mark.asyncio
async def test_integration_with_emulator():
    broker = PubSubBroker(project_id="test-project")
    processed = asyncio.Event()

    @broker.subscriber(
        alias="integration-handler",
        topic_name="events",
        subscription_name="events-subscription",
    )
    async def handle(message: Message):
        processed.set()

    await broker.start()
    try:
        await broker.publish(topic_name="events", data={"hello": "world"})
        await asyncio.wait_for(processed.wait(), timeout=5)
    finally:
        await broker.shutdown()
```

---

## Testing Middlewares

Middlewares can be tested the same way as handlers. Register the middleware on the broker or router and assert it was invoked in your test.

---

## Best Practices

### Use Fixtures for Common Setup

```python
--8<-- "testing/e2_06_testing_fixtures.py:broker_fixture"

--8<-- "testing/e2_06_testing_fixtures.py:client_fixture"

--8<-- "testing/e2_06_testing_fixtures.py:fixture_test"
```

### Clear State Between Tests

```python
--8<-- "testing/e2_06_testing_fixtures.py:clear_state_fixture"
```

### Use Parametrized Tests

```python
--8<-- "testing/e2_06_testing_fixtures.py:parametrized_test"
```

### Test Concurrent Processing

```python
import asyncio

@pytest.mark.asyncio
async def test_concurrent_processing(test_client):
    # Publish multiple messages concurrently
    await asyncio.gather(*[
        test_client.publish("events", data=f"message-{i}".encode())
        for i in range(10)
    ])

    # Assert all were processed correctly
```

---

## pytest Configuration

**pytest.ini:**

```ini
[pytest]
asyncio_mode = auto
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
```

**Running tests:**

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=fastpubsub --cov-report=html

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/
```

---

## Recap

- **PubSubTestClient**: Use for fast, in-memory unit tests without the emulator
- **Mocking**: Mock publishers and external dependencies for isolated testing
- **Middleware testing**: Test middlewares both in isolation and integrated with subscribers
- **Integration tests**: Use the Pub/Sub emulator for end-to-end testing
- **Best practices**: Use fixtures, clear state between tests, and test both success and failure paths
