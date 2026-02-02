"""Title: Publisher Instance API

Demonstrates creating a reusable Publisher instance bound to a specific topic.

This example shows:
- Creating a Publisher instance with broker.publisher()
- Binding the publisher to a specific topic at creation time
- Publishing messages without specifying the topic each time

Using a Publisher instance is useful when you need to publish multiple messages
to the same topic, as it avoids repeating the topic name.

Run with:
    fastpubsub run examples.basic_usage.e2_02_basic_publisher:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)

publisher: Publisher = broker.publisher("test-topic")


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-publish",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish() -> None:
    await publisher.publish({"hello": "world"})
