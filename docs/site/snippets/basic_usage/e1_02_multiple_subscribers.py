"""Title: Multiple Subscribers on Different Topics

Demonstrates how to create multiple subscribers within a single FastPubSub application.

This example shows:
- Creating multiple subscribers with different aliases
- Subscribing to the same topic with different subscription names
- Subscribing to different topics
- How messages are routed to the appropriate handlers

The example creates three subscribers:
1. 'first-alias' - listens to 'first-topic'
2. 'second-alias' - also listens to 'first-topic' (different subscription)
3. 'third-alias' - listens to 'second-topic'

Run with:
    fastpubsub run examples.basic_usage.e1_02_multiple_subscribers:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "first-alias",
    topic_name="first-topic",
    subscription_name="test-multi-subscription",
)
async def handle_response(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@broker.subscriber(
    "second-alias",
    topic_name="first-topic",
    subscription_name="test-multi-subscription2",
)
async def handle_response_another_subscription(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@broker.subscriber(
    "third-alias",
    topic_name="second-topic",
    subscription_name="test-multi-subscription3",
)
async def handle_another_response_another_topic(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("first-topic", {"hello": "world"})
    await broker.publish("second-topic", {"foo": "bar"})
