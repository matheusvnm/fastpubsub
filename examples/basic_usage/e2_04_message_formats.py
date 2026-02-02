"""Title: Different Message Format Support

Demonstrates publishing messages in various formats supported by FastPubSub.

This example shows:
- Publishing Pydantic models (automatically serialized to JSON)
- Publishing dictionaries (serialized to JSON)
- Publishing strings (encoded to bytes)
- Publishing raw bytes

FastPubSub automatically handles serialization for different data types,
making it easy to work with structured data using Pydantic models while
also supporting simple string and bytes payloads.

Run with:
    fastpubsub run examples.basic_usage.e2_04_message_formats:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from pydantic import BaseModel

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger


class TestMessage(BaseModel):
    event: str
    source: str
    message: str


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic-pydantic",
    subscription_name="test-publish-pydantic",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message.data.decode()}")


@app.after_startup
async def test_publish() -> None:
    message = TestMessage(
        event="checkout", source="checkout-cart", message="the user put a item to the cart"
    )

    publisher: Publisher = broker.publisher("test-topic-pydantic")
    await publisher.publish(message)
    await publisher.publish({"some_dict": "dict_data"})
    await publisher.publish("some_string_text")
    await publisher.publish(b"some_byte_text")
