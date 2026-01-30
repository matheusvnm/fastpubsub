"""Title: Basic Message Publishing

Demonstrates the simplest way to publish messages using broker.publish().

This example shows:
- Publishing messages directly via broker.publish()
- Passing the topic name and data as arguments
- Using the after_startup hook to publish a test message

The example publishes a dictionary message to 'test-topic' which is then
received by the subscriber handler.

Run with:
    fastpubsub run examples.basic_usage.e2_01_basic_publisher:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-publish",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", {"hello": "world"})
