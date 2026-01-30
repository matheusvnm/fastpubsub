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

### Basic Example

```python
import pytest
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)

@broker.subscriber(
    alias="user-handler",
    topic_name="user-events",
    subscription_name="user-events-subscription",
)
async def handle_user_event(message: Message):
    data = message.data.decode("utf-8")
    return f"Processed: {data}"

@pytest.mark.asyncio
async def test_user_event_handler():
    async with PubSubTestClient(broker) as client:
        # Publish a test message
        await client.publish("user-events", data=b"test-data")

        # The subscriber automatically processes the message
        # Add assertions on side effects (database, mocks, etc.)
```

### Testing with Pydantic Models

```python
import pytest
from pydantic import BaseModel
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.testing import PubSubTestClient

class User(BaseModel):
    name: str
    email: str
    age: int

broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)

processed_users = []

@broker.subscriber(
    alias="user-processor",
    topic_name="user-created",
    subscription_name="user-created-subscription",
)
async def process_new_user(message: Message):
    user = User.model_validate_json(message.data)
    processed_users.append(user)

@pytest.mark.asyncio
async def test_process_new_user():
    processed_users.clear()

    async with PubSubTestClient(broker) as client:
        test_user = User(name="Alice", email="alice@example.com", age=30)

        # Publish using Pydantic model (auto-serialized)
        await client.publish("user-created", data=test_user)

        # Verify the user was processed
        assert len(processed_users) == 1
        assert processed_users[0].name == "Alice"
```

### Testing Exception Handling

```python
import pytest
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)

@broker.subscriber(
    alias="validation-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def validate_event(message: Message):
    data = message.data.decode("utf-8")

    if data == "invalid":
        raise Drop("Invalid message format")

    if data == "retry":
        raise Retry("Temporary failure")

    return "Success"

@pytest.mark.asyncio
async def test_drop_exception():
    async with PubSubTestClient(broker) as client:
        # This should not raise - Drop is handled gracefully
        await client.publish("events", data=b"invalid")

@pytest.mark.asyncio
async def test_retry_exception():
    async with PubSubTestClient(broker) as client:
        # Retry exceptions are also handled
        await client.publish("events", data=b"retry")
```

---

## Mocking

### Mocking Publishers

When testing code that publishes messages, mock the publisher to avoid actual publishing:

```python
import pytest
from unittest.mock import AsyncMock, patch
from fastpubsub import PubSubBroker

broker = PubSubBroker(project_id="test-project")

class UserService:
    def __init__(self, broker):
        self.user_publisher = broker.publisher("user-events")

    async def create_user(self, name: str, email: str):
        user_data = {"name": name, "email": email}
        await self.user_publisher.publish(data=user_data)
        return user_data

@pytest.mark.asyncio
async def test_user_service_publishes_event():
    mock_publisher = AsyncMock()

    with patch.object(broker, 'publisher', return_value=mock_publisher):
        service = UserService(broker)
        await service.create_user("Bob", "bob@example.com")

        mock_publisher.publish.assert_called_once()
        call_kwargs = mock_publisher.publish.call_args.kwargs
        assert call_kwargs["data"]["name"] == "Bob"
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
import pytest
from fastpubsub import PubSubBroker, PubSubRouter, Message
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="test-project")
users_router = PubSubRouter(prefix="users")

processed_events = []

@users_router.subscriber(
    alias="created",
    topic_name="user-created",
    subscription_name="user-created-subscription",
)
async def handle_user_created(message: Message):
    processed_events.append("created")

broker.include_router(users_router)

@pytest.mark.asyncio
async def test_router_subscriber():
    processed_events.clear()

    async with PubSubTestClient(broker) as client:
        await client.publish("user-created", data=b"test")

        assert "created" in processed_events
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
import pytest
import asyncio
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)

received_messages = []

@broker.subscriber(
    alias="integration-handler",
    topic_name="integration-test-topic",
    subscription_name="integration-test-subscription",
    autocreate=True,
)
async def handle_message(message: Message):
    received_messages.append(message.data.decode("utf-8"))

@pytest.mark.asyncio
async def test_end_to_end_message_flow():
    received_messages.clear()

    await broker.start()

    try:
        await broker.publish("integration-test-topic", data=b"integration-test-data")

        # Wait for message to be processed
        await asyncio.sleep(1)

        assert "integration-test-data" in received_messages
    finally:
        await broker.shutdown()
```

---

## Best Practices

### Use Fixtures for Common Setup

```python
import pytest
from fastpubsub import PubSubBroker
from fastpubsub.testing import PubSubTestClient

@pytest.fixture
async def test_broker():
    broker = PubSubBroker(project_id="test-project")
    yield broker
    await broker.shutdown()

@pytest.fixture
async def test_client(test_broker):
    async with PubSubTestClient(test_broker) as client:
        yield client

@pytest.mark.asyncio
async def test_with_fixtures(test_client):
    await test_client.publish("topic", data=b"test")
```

### Clear State Between Tests

```python
processed_messages = []

@pytest.fixture(autouse=True)
def clear_state():
    """Automatically clear state before each test."""
    processed_messages.clear()
    yield
    processed_messages.clear()
```

### Use Parametrized Tests

```python
@pytest.mark.parametrize("message_data,expected_result", [
    (b"valid", "processed"),
    (b"invalid", "dropped"),
    (b"retry", "retried"),
])
@pytest.mark.asyncio
async def test_message_processing(test_client, message_data, expected_result):
    await test_client.publish("events", data=message_data)
    # Assert expected_result
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
