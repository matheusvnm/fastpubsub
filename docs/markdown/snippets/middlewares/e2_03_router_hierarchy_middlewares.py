"""Title: Middleware Order Independence

Demonstrates that middleware and routers can be added in any order.

This example shows:
- Adding routers before adding middleware
- Adding middleware after router relationships are established
- The final middleware chain is the same regardless of configuration order

FastPubSub resolves the middleware hierarchy at runtime, so you can configure
brokers, routers, and middleware in whatever order is most convenient for
your application structure.

Run with:
    fastpubsub run examples.middlewares.e2_03_router_hierarchy_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from snippets.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

# It works in any order!!!
child_router = PubSubRouter(prefix="subrouter")
parent_router = PubSubRouter(prefix="router")
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
)
broker.include_router(parent_router)
parent_router.include_router(child_router)

child_router.include_middleware(SubRouterMiddleware)
parent_router.include_middleware(RouterMiddleware)
broker.include_middleware(BrokerMiddleware)


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
