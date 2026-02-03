"""Title: Basic Subscriber Setup

Demonstrates the simplest way to create a Pub/Sub subscriber with FastPubSub.

This example shows:
- Creating a PubSubBroker with a project ID
- Defining a subscriber using the @broker.subscriber decorator
- Handling incoming messages with an async handler function
- Publishing a test message on application startup

The subscriber listens to 'subscriber-topic' and logs each received message.

Run with:
    fastpubsub run examples.basic_usage.e1_01_basic_subscriber:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

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


@app.after_startup
async def test_publish() -> None:
    await broker.publish("subscriber-topic", {"message": "streaming a message"})
