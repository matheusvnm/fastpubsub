"""Example: FastAPI integration.

This example shows how to integrate FastPubSub with a FastAPI application,
allowing you to have both HTTP endpoints and Pub/Sub subscribers in the
same application. FastPubSub extends FastAPI, so you can use decorators
like @app.post() alongside @broker.subscriber().

The HTTP endpoint receives a user message and publishes it to a topic,
while the subscriber processes messages from that topic.

Run with: fastpubsub run examples.basic_usage.e4_01_basic_fastapi_integration:app
"""

from pydantic import BaseModel

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger


class UserMessage(BaseModel):
    user_id: int
    message: str


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-basic-subscription003",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.post("/user/send-message")
async def send_message(user_message: UserMessage) -> dict[str, str]:
    publisher: Publisher = broker.publisher("test-topic")
    await publisher.publish(user_message.model_dump())

    return {"response": "ok"}
