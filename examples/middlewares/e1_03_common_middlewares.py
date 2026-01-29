"""Title: Built-in GZip Middleware

Demonstrates using FastPubSub's built-in GZipMiddleware for message compression.

This example shows:
- Using the built-in GZipMiddleware
- Adding middleware with configuration parameters (compresslevel)
- How compressed messages automatically get 'content-encoding' attribute

The GZipMiddleware compresses message data before publishing and adds
a 'content-encoding' attribute. This reduces bandwidth usage for large messages.

Run with:
    fastpubsub run examples.middlewares.e1_03_common_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares import GZipMiddleware

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_middleware(GZipMiddleware, compresslevel=2)

app = FastPubSub(broker)


@broker.subscriber(
    "gzipped_message_handler",
    topic_name="gzipped_topic",
    subscription_name="gzipped_sub",
)
async def broker_gzip_message(message: Message) -> None:
    logger.info(f"We received message with encoding {message.attributes['content-encoding']}")


@app.after_startup
async def test_publish() -> None:
    publisher = broker.publisher("gzipped_topic")
    await publisher.publish("Hi!")
