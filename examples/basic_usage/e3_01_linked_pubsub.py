from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.pubsub.publisher import Publisher

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "first-alias",
    topic_name="first-topic",
    subscription_name="test-publish",
)
async def handle(message: Message):
    logger.info(f"Processed message: {message.data.decode()}")
    publisher: Publisher = broker.publisher("second-topic")
    await publisher.publish({"foo": "bar"})


@broker.subscriber(
    "second-alias",
    topic_name="second-topic",
    subscription_name="test-linked-subscription",
)
async def handle_from_another_topic(message: Message):
    logger.info(f"Received message from the first-topic: {message}")


@app.after_startup
async def test_publish():
    publisher: Publisher = broker.publisher("first-topic")
    await publisher.publish({"hello": "world"})
