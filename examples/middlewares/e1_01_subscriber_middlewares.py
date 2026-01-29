"""Title: Subscriber Middleware Hierarchy

Demonstrates middleware execution at different levels: broker, router, and subscriber.

This example shows:
- Adding middleware to a broker (affects all handlers)
- Adding middleware to a router (affects router handlers and children)
- Adding middleware to a specific subscriber (affects only that handler)
- How middleware cascades through the hierarchy

The middleware execution order for messages going through the hierarchy:
1. Broker middleware runs first
2. Router middleware runs for router subscribers
3. Subscriber middleware runs for specific handlers

Run with:
    fastpubsub run examples.middlewares.e1_01_subscriber_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubcriberMiddleware
from fastpubsub import FastPubSub, Message, Middleware, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

router = PubSubRouter(prefix="myawesomerouter", middlewares=[Middleware(RouterMiddleware)])
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(BrokerMiddleware)],
    routers=[router],
)
app = FastPubSub(broker)


@broker.subscriber(
    "broker-subscriber",
    topic_name="topic_one_mid",
    subscription_name="subscription_one_mid",
)
async def broker_handle(_: Message) -> None:
    logger.info("This handler has only the broker middleware")
    await broker.publish(topic_name="topic_two_mid", data={"C": "D"})


@router.subscriber(
    "router-subscriber",
    topic_name="topic_two_mid",
    subscription_name="subscription_two_mid",
)
async def router_handle(_: Message) -> None:
    logger.info("This handler has a router and broker middlewares")
    await router.publish(topic_name="topic_three_mid", data={"F": "G"})


@router.subscriber(
    "router-subscriber-with-mid",
    topic_name="topic_three_mid",
    subscription_name="subscription_three_mid",
    middlewares=[Middleware(SubcriberMiddleware)],
)
async def router_handle_with_middleware(_: Message) -> None:
    logger.info("This handler has all middlewares")


@app.after_startup
async def after_started() -> None:
    await broker.publish(topic_name="topic_one_mid", data={"A": "B"})
