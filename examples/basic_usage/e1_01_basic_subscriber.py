from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker._add_subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
)
async def handle(message: Message):
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish():
    await broker.publish("test-topic", {"hello": "world"})
