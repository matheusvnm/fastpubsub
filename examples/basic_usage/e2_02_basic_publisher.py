"""Example: Publisher with explicit topic declaration.

This example demonstrates creating a Publisher object for a specific
topic, which can be reused for multiple publishes. This is useful when
you need to publish to the same topic multiple times.

Run with: fastpubsub run examples.basic_usage.e2_02_basic_publisher:app
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
