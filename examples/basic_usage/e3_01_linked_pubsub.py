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
