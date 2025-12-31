"""Example: Publisher middlewares.

This example demonstrates how to apply middlewares to publishers,
allowing you to intercept and modify messages before they are sent.
Publisher middlewares can be added using publisher.include_middleware().

The example shows:
- Router-level middleware affecting router.publish()
- Publisher-specific middleware via include_middleware()

Run with: fastpubsub run examples.middlewares.e1_02_publisher_middlewares:app
"""

from examples.middlewares.middlewares import PublisherMiddleware, RouterMiddleware
from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

router = PubSubRouter(prefix="core", middlewares=[RouterMiddleware])
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


publisher = broker.publisher("topic_a")
publisher.include_middleware(PublisherMiddleware)


@app.after_startup
async def test_publish() -> None:
    await router.publish(topic_name="topic_b", data={"some_message": "messageB"})
    await publisher.publish(data={"some_message": "messageA"})
