"""Example: Basic subscriber setup.

This example demonstrates the simplest way to create a message subscriber
with FastPubSub. It shows how to:

1. Create a PubSubBroker with your Google Cloud project ID
2. Create a FastPubSub application
3. Define a subscriber handler using the @broker.subscriber decorator
4. Access the raw Message object in your handler

Run with: fastpubsub run examples.basic_usage.e1_01_basic_subscriber:app
"""

import asyncio

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "subscriber-alias",
    topic_name="subscriber-topic",
    subscription_name="subscriber-subscription",
)
async def process_message(message: Message) -> None:
    logger.info(f"Processed message: {message}")
    await asyncio.sleep(1)


@app.after_startup
async def test_publish() -> None:
    sent = 0
    max_messages = 100
    while sent < max_messages:
        await broker.publish("subscriber-topic", {"message": "streaming a message"})
        sent += 1
        # await asyncio.sleep(0.1)
