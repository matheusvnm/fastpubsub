"""Example: Basic message publishing.

This example shows how to publish messages to a Pub/Sub topic using
the broker's publish method. Messages can be dictionaries, strings,
or bytes.

Run with: fastpubsub run examples.basic_usage.e2_01_basic_publisher:app
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
