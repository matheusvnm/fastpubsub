"""Example: Controlling subscriber concurrency with max_messages.

This example demonstrates how to limit the number of messages a subscriber
processes concurrently using the max_messages parameter. This is useful
for controlling resource usage and preventing overload when handlers
perform expensive operations.

In this example, max_messages=10 means the subscriber will process at most
10 messages simultaneously, even though 50 messages are published.

Run with: fastpubsub run examples.basic_usage.e5_01_subscribers_max_messages:app
"""

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
