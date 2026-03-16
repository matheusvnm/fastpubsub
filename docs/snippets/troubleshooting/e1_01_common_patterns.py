import asyncio

from pydantic import BaseModel, ValidationError

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Drop
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


class SomeModel(BaseModel):
    value: str


# --8<-- [start:validation_error_handling]
@broker.subscriber(
    alias="validated-handler",
    topic_name="validated-events",
    subscription_name="validated-events-subscription",
)
async def validated_handler(message: Message):
    try:
        _ = SomeModel.model_validate_json(message.data)
    except ValidationError as e:  # Any other error will nack the message
        logger.exception("Invalid message format")
        raise Drop("Invalid message format") from e


# --8<-- [end:validation_error_handling]


# --8<-- [start:ack_deadline_handler]
@broker.subscriber(
    alias="slow-handler",
    topic_name="slow-topic",
    subscription_name="slow-subscription",
    ack_deadline_seconds=120,  # Increase deadline
)
async def slow_handler(message: Message):
    await asyncio.sleep(60)  # Takes longer than default 60s


# --8<-- [end:ack_deadline_handler]


# --8<-- [start:non_blocking]
# BAD: Blocks event loop
# time.sleep(5)

# GOOD: Non-blocking
# await asyncio.sleep(5)
# --8<-- [end:non_blocking]


# Simulated Redis client for idempotent handler example
class MockRedis:
    _store: dict = {}

    async def exists(self, key: str) -> bool:
        return key in self._store

    async def set(self, key: str, value: str, ex: int = 0) -> None:
        self._store[key] = value


redis = MockRedis()


async def do_work(data: bytes) -> None:
    """Process the message data."""
    pass


# --8<-- [start:idempotent_handler]
@broker.subscriber(
    alias="idempotent-handler",
    topic_name="idempotent-events",
    subscription_name="idempotent-events-subscription",
)
async def idempotent_handler(message: Message):
    event_id = message.attributes.get("event_id")

    if await redis.exists(f"processed:{event_id}"):
        return  # Already handled

    await do_work(message.data)
    await redis.set(f"processed:{event_id}", "1", ex=86400)


# --8<-- [end:idempotent_handler]


# --8<-- [start:exactly_once_handler]
@broker.subscriber(
    alias="exactly-once-handler",
    topic_name="exactly-once-events",
    subscription_name="exactly-once-subscription",
    enable_exactly_once_delivery=True,
)
async def exactly_once_handler(message: Message):
    pass


# --8<-- [end:exactly_once_handler]
