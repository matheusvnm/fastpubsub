from pydantic import BaseModel, Field

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger


class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])


# --8<-- [start:basic_subscriber_setup]
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)
# --8<-- [end:basic_subscriber_setup]


# --8<-- [start:basic_subscriber]
@broker.subscriber(
    alias="my_handler",
    topic_name="in_topic",
    subscription_name="sub_name",
)
async def handle_message(message: Message):
    logger.info(f"The message {message.id} is processed.")
    await broker.publish(topic_name="out_topic", data="Hi!")
# --8<-- [end:basic_subscriber]

# --8<-- [start:basic_subscriber_startup]
@app.after_startup
async def publish_initial_message() -> None:
    address = Address(street="Av. Flores", number="213")
    await broker.publish(topic_name="in_topic", data=address)
# --8<-- [end:basic_subscriber_startup]
