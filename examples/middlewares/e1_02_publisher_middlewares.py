"""Title: Publisher Middleware

Demonstrates how to add middleware specifically to publishers.

This example shows:
- Creating a Publisher instance with broker.publisher()
- Adding middleware to a publisher using publisher.include_middleware()
- Publisher middleware only runs on publish operations, not on receive

This is useful for:
- Adding logging or metrics to outgoing messages
- Transforming message data before publishing
- Adding headers or attributes to all published messages

Run with:
    fastpubsub run examples.middlewares.e1_02_publisher_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from examples.middlewares.middlewares import PublisherMiddleware, RouterMiddleware
from fastpubsub import FastPubSub, Message, Middleware, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

router = PubSubRouter(prefix="core", middlewares=[Middleware(RouterMiddleware)])
broker = PubSubBroker(project_id="fastpubsub-pubsub-local", routers=[router])
app = FastPubSub(broker)


@router.subscriber(
    "router-subscriber",
    topic_name="topic_b",
    subscription_name="subscription_b",
)
async def router_handle(message: Message) -> None:
    logger.info(f"We received message {message} on router_handle")


@broker.subscriber(
    "broker-subscriber",
    topic_name="topic_a",
    subscription_name="subscription_a",
)
async def broker_handle(message: Message) -> None:
    logger.info(f"We received message {message} on broker_handle")
    await router.publish(topic_name="topic_b", data={"some_message": "messageB"})


publisher = broker.publisher("topic_a")
publisher.include_middleware(PublisherMiddleware)


@app.after_startup
async def test_publish() -> None:
    await publisher.publish(data={"some_message": "messageA"})
