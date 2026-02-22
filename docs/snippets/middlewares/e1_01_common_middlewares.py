from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares import GZipMiddleware

# --8<-- [start:gzip_middleware_setup]
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_middleware(GZipMiddleware, compresslevel=2)
# --8<-- [end:gzip_middleware_setup]

app = FastPubSub(broker)


@broker.subscriber(
    "gzipped_message_handler",
    topic_name="gzipped_topic",
    subscription_name="gzipped_sub",
)
async def broker_gzip_message(message: Message) -> None:
    logger.info(f"We received message with encoding {message.attributes['content-encoding']}")


@app.after_startup
async def publish_first_message() -> None:
    publisher = broker.publisher("gzipped_topic")
    await publisher.publish("Hi!")
