import pytest

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.testing import PubSubTestClient

# --8<-- [start:exception_setup]
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


# --8<-- [end:exception_setup]


# --8<-- [start:drop_test]
@pytest.mark.asyncio
async def test_drop_exception():
    async with PubSubTestClient(broker) as client:
        # This should not raise - Drop is handled gracefully
        await client.publish(b"invalid", topic="events")


# --8<-- [end:drop_test]


# --8<-- [start:retry_test]
@pytest.mark.asyncio
async def test_retry_exception():
    async with PubSubTestClient(broker) as client:
        # Retry exceptions are also handled
        await client.publish(b"retry", topic="events")


# --8<-- [end:retry_test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

