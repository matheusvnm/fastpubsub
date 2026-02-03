"""Title: Subscriber Concurrency Control

Demonstrates how to limit concurrent message processing with max_messages.

This example shows:
- Using the max_messages parameter to control concurrency
- Limiting how many messages a subscriber processes simultaneously
- Using asyncio TaskGroup for bulk publishing

The subscriber is configured with max_messages=10, meaning it will only
process up to 10 messages concurrently. The example publishes 50 messages
(5x the limit) to demonstrate the throttling behavior.

This is useful for:
- Preventing resource exhaustion
- Rate limiting expensive operations
- Controlling memory usage with large message volumes

Run with:
    fastpubsub run examples.basic_usage.e5_01_subscribers_max_messages:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
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
