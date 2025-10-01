from pydantic import BaseModel

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.pubsub.publisher import Publisher


class UserMessage(BaseModel):
    user_id: int
    message: str


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker._add_subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-basic-subscription003",
)
async def handle(message: Message):
    logger.info(f"Processed message: {message}")


@app.post("/user/send-message")
async def send_message(user_message: UserMessage):
    publisher: Publisher = broker.publisher("test-topic")
    await publisher.publish(user_message.model_dump())

    return {"response": "ok"}
