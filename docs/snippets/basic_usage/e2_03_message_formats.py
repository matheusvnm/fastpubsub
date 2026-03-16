from pydantic import BaseModel

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:pydantic_model]
class TestMessage(BaseModel):
    event: str
    source: str
    message: str


# --8<-- [end:pydantic_model]


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic-pydantic",
    subscription_name="test-publish-pydantic",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message.data.decode()}")


# --8<-- [start:publish_formats]
@app.after_startup
async def publish_initial_messages() -> None:
    message = TestMessage(
        event="checkout", source="checkout-cart", message="the user put a item to the cart"
    )

    publisher: Publisher = broker.publisher("test-topic-pydantic")
    await publisher.publish(message)  # Pydantic model
    await publisher.publish({"some_dict": "dict_data"})  # Dictionary
    await publisher.publish("some_string_text")  # String
    await publisher.publish(b"some_byte_text")  # Bytes


# --8<-- [end:publish_formats]
