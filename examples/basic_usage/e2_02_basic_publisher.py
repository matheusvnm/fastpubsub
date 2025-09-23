

from fastpubsub.applications import FastPubSub
from fastpubsub.logger import logger
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.pubsub.publisher import Publisher


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)

publisher: Publisher = broker.publisher("test-topic")

@broker._add_subscriber("test-alias",
                   topic_name="test-topic",
                   subscription_name="test-publish",)
async def handle(message: Message):
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish():
    await publisher.publish({"hello": "world"})
