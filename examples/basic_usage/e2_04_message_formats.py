"""Example: Different message formats.

This example shows the various data formats you can publish:
- Pydantic models (automatically serialized via model_dump)
- Dictionaries (JSON-serialized)
- Strings (UTF-8 encoded)
- Raw bytes (sent as-is)

FastPubSub's serialization system automatically handles encoding
based on the data type provided.

Run with: fastpubsub run examples.basic_usage.e2_04_message_formats:app
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
