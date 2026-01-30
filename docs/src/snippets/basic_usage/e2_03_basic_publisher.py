"""Title: Publisher in Startup Hook

Demonstrates creating a Publisher instance inside the startup hook.

This example shows:
- Deferring Publisher instantiation until application startup
- Creating a Publisher within the after_startup hook
- Publishing messages immediately after the publisher is created

This pattern is useful when you need the application to be fully initialized
before creating publishers, or when publisher configuration depends on
runtime state.

Run with:
    fastpubsub run examples.basic_usage.e2_03_basic_publisher:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
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
    publisher: Publisher = broker.publisher("test-topic")
    await publisher.publish({"hello": "world"})
