import pytest

from fastpubsub import PubSubBroker
from fastpubsub.testing import PubSubTestClient

processed_messages: list[str] = []


# --8<-- [start:broker_fixture]
@pytest.fixture
async def test_broker():
    broker = PubSubBroker(project_id="test-project")
    yield broker
    await broker.shutdown()


# --8<-- [end:broker_fixture]


# --8<-- [start:client_fixture]
@pytest.fixture
async def test_client(test_broker: PubSubBroker):
    async with PubSubTestClient(test_broker) as client:
        yield client


# --8<-- [end:client_fixture]


# --8<-- [start:clear_state_fixture]
@pytest.fixture(autouse=True)
def clear_state():
    """Automatically clear state before each test."""
    processed_messages.clear()
    yield
    processed_messages.clear()


# --8<-- [end:clear_state_fixture]


# --8<-- [start:fixture_test]
@pytest.mark.asyncio
async def test_with_fixtures(test_client: PubSubTestClient):
    await test_client.publish(b"test", topic="topic")


# --8<-- [end:fixture_test]


# --8<-- [start:parametrized_test]
@pytest.mark.parametrize(
    "message_data,expected_result",
    [
        (b"valid", "processed"),
        (b"invalid", "dropped"),
        (b"retry", "retried"),
    ],
)
@pytest.mark.asyncio
async def test_message_processing(
    test_client: PubSubTestClient, message_data: bytes, expected_result: str
):
    await test_client.publish(message_data, topic="events")
    # Assert expected_result based on your logic


# --8<-- [end:parametrized_test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
