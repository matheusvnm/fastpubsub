"""Example: Subscriber-level middlewares.

This example shows how middlewares are applied at different levels:
- Broker middlewares: Applied to all subscribers in the broker
- Router middlewares: Applied to subscribers in that router
- Subscriber middlewares: Applied to a specific subscriber only

The middleware execution order is: Broker -> Router -> Subscriber -> Handler

Run with: fastpubsub run examples.middlewares.e1_01_subscriber_middlewares:app
"""

from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubcriberMiddleware
from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

router = PubSubRouter(prefix="myawesomerouter", middlewares=[RouterMiddleware])
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local", middlewares=[BrokerMiddleware], routers=[router]
)
app = FastPubSub(broker)


@broker.subscriber(
    "broker-subscriber",
    topic_name="topic_one_mid",
    subscription_name="subscription_one_mid",
)
async def broker_handle(_: Message) -> None:
    logger.info("This handler has only the broker middleware")


@router.subscriber(
    "router-subscriber",
    topic_name="topic_two_mid",
    subscription_name="subscription_two_mid",
)
async def router_handle(_: Message) -> None:
    logger.info("This handler has a router and broker middlewares")


@router.subscriber(
    "router-subscriber-with-mid",
    topic_name="topic_three_mid",
    subscription_name="subscription_three_mid",
    middlewares=[SubcriberMiddleware],
)
async def router_handle_with_middleware(_: Message) -> None:
    logger.info("This handler has all middlewares")


@app.after_startup
async def after_started() -> None:
    await broker.publish(topic_name="topic_one_mid", data={"A": "B"})
    await broker.publish(topic_name="topic_two_mid", data={"C": "D"})
    await broker.publish(topic_name="topic_three_mid", data={"F": "G"})
