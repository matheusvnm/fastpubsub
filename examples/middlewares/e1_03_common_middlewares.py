from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares.gzip import GZipMiddleware

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_middleware(GZipMiddleware)

app = FastPubSub(broker)


@broker.subscriber(
    "gzipped_message_handler",
    topic_name="gzipped_topic",
    subscription_name="gzipped_sub",
)
async def broker_gzip_message(message: Message) -> None:
    logger.info(f"We received message with encoding {message.attributes['Content-Encoding']}")


@app.after_startup
async def test_publish() -> None:
    publisher = broker.publisher("gzipped_topic")
    await publisher.publish("Hi!")
