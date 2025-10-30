import asyncio
import random
from asyncio import TaskGroup

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


MAX_MESSAGES = 10


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
    max_messages=MAX_MESSAGES,
)
async def process_message(message: Message) -> None:
    logger.info(f"Processed message: {message}")
    value = random.randint(1, 5)
    await asyncio.sleep(value)


@app.after_startup
async def test_publish() -> None:
    async with TaskGroup() as tg:
        for _ in range(MAX_MESSAGES * 5):
            tg.create_task(broker.publish("test-topic", "hi!"))
