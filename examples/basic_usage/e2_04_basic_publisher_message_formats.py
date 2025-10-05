from pydantic import BaseModel

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.pubsub.publisher import Publisher


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
    await publisher.publish(bytearray("some_string_as_bytearray", "utf-8"))
