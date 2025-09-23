from fastpubsub.logger import logger
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker._add_subscriber("first-alias",
                   topic_name="first-topic",
                   subscription_name="test-multi-subscription",)
async def handle_response(message: Message):
    logger.info(f"Processed message: {message}")


@broker._add_subscriber("second-alias",
                   topic_name="first-topic",
                   subscription_name="test-multi-subscription2",)
async def handle_response_another_subscription(message: Message):
    logger.info(f"Processed message: {message}")


@broker._add_subscriber("third-alias",
                   topic_name="second-topic",
                   subscription_name="test-multi-subscription3",)
async def handle_another_response_another_topic(message: Message):
    logger.info(f"Processed message: {message}")



@app.after_startup
async def test_publish():
    await broker.publish("first-topic", {"hello": "world"})
    await broker.publish("second-topic", {"foo": "bar"})
