"""Example: Built-in GZip middleware.

This example demonstrates using FastPubSub's built-in GZipMiddleware
to automatically compress messages before publishing and decompress
them when received. This is useful for reducing network bandwidth
when dealing with large messages.

The Content-Encoding attribute is automatically set to 'gzip' when
the middleware compresses a message.

Run with: fastpubsub run examples.middlewares.e1_03_common_middlewares:app
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.middlewares import GZipMiddleware

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
