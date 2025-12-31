"""Example: Multiple subscribers on different topics.

This example shows how to define multiple subscribers within the same
application, each listening to different topics or with different
subscriptions. FastPubSub manages all subscribers concurrently.

Run with: fastpubsub run examples.basic_usage.e1_02_multiple_subscribers:app
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
