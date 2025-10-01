from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares.gzip import GZipMiddleware

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_middleware(GZipMiddleware)

app = FastPubSub(broker)


@broker._add_subscriber(
    "broker-subscriber-2",
    topic_name="topic_a_2",
    subscription_name="subscription_a",
)
async def broker_gzip_message(message: Message):
    logger.info(f"We received message with encoding {message.attributes['Content-Encoding']}")


@app.after_startup
async def test_publish():
    publisher = broker.publisher("topic_a_2")
    await publisher.publish("Hi!")
