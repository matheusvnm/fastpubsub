"""Title: Router Middleware at Initialization

Demonstrates adding middleware to routers at initialization time.

This example shows:
- Creating nested routers (child_router inside parent_router)
- Adding middleware via the middlewares parameter in constructors
- How middleware cascades through nested router hierarchy

The hierarchy is:
- Broker (with BrokerMiddleware)
  - Parent Router (with RouterMiddleware)
    - Child Router (with SubRouterMiddleware)

Each level adds its middleware to the execution chain.

Run with:
    fastpubsub run examples.middlewares.e2_01_router_hierarchy_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware
from fastpubsub import FastPubSub, Message, Middleware, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

child_router = PubSubRouter(prefix="subrouter", middlewares=[Middleware(SubRouterMiddleware)])
parent_router = PubSubRouter(
    prefix="router", routers=[child_router], middlewares=[Middleware(RouterMiddleware)]
)
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(BrokerMiddleware)],
    routers=[parent_router],
)
app = FastPubSub(broker)


@broker.subscriber(
    "broker-subscriber",
    topic_name="some_test_topic",
    subscription_name="tst_sub",
)
async def broker_handle(_: Message) -> None:
    logger.info("We received a message!")
    await parent_router.publish(topic_name="some_test_topic2", data={"C": "D"})


@parent_router.subscriber(
    "parent-subscriber",
    topic_name="some_test_topic2",
    subscription_name="tst_sub",
)
async def parent_router_handle(_: Message) -> None:
    logger.info("We received a message!")
    await child_router.publish(topic_name="some_test_topic3", data={"E": "F"})


@child_router.subscriber(
    "child-subscriber",
    topic_name="some_test_topic3",
    subscription_name="tst_sub",
)
async def subrouter_handle(_: Message) -> None:
    logger.info("We received a message!")


@app.after_startup
async def after_started() -> None:
    await broker.publish(topic_name="some_test_topic", data={"A": "B"})
