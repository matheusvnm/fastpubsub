"""Title: Message Chaining Between Topics

Demonstrates how a subscriber can publish messages to another topic.

This example shows:
- Chaining message processing between topics
- A subscriber that receives a message and publishes to a different topic
- Creating pipelines where one message triggers subsequent messages

The flow:
1. Initial message is published to 'first-topic'
2. First subscriber processes it and publishes to 'second-topic'
3. Second subscriber receives the chained message

This pattern is useful for event-driven architectures and workflow processing.

Run with:
    fastpubsub run examples.basic_usage.e3_01_linked_pubsub:app

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
    subscription_name="test-publish",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message.data.decode()}")
    publisher = broker.publisher("second-topic")
    await publisher.publish({"foo": "bar"})


@broker.subscriber(
    "second-alias",
    topic_name="second-topic",
    subscription_name="test-linked-subscription",
)
async def handle_from_another_topic(message: Message) -> None:
    logger.info(f"Received message from the first-topic: {message}")


@app.after_startup
async def test_publish() -> None:
    publisher = broker.publisher("first-topic")
    await publisher.publish({"hello": "world"})
