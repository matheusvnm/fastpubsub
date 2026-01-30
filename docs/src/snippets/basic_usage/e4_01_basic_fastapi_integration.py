"""Title: FastAPI Integration

Demonstrates how to integrate FastPubSub with HTTP endpoints using FastAPI-style routing.

This example shows:
- Defining HTTP POST endpoints with @app.post decorator
- Publishing messages to Pub/Sub from HTTP request handlers
- Using Pydantic models for request body validation
- Combining HTTP API and Pub/Sub subscriber in the same application

When a POST request is made to /user/send-message with a UserMessage payload,
the handler publishes the message to Pub/Sub, which is then received by
the subscriber.

Run with:
    fastpubsub run examples.basic_usage.e4_01_basic_fastapi_integration:app

Test the endpoint:
    curl -X POST http://localhost:8000/user/send-message \\
         -H "Content-Type: application/json" \\
         -d '{"user_id": 1, "message": "Hello!"}'

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
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
