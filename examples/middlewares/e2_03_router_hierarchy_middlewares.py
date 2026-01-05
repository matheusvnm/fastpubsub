"""Example: Flexible middleware and router registration order.

This example demonstrates that middlewares and routers can be
registered in any order. The comment "It works in any order!!!"
highlights that you can:
1. Include routers before adding middlewares
2. Add middlewares after the hierarchy is established

FastPubSub resolves the correct middleware chain at runtime,
regardless of registration order.

Run with: fastpubsub run examples.middlewares.e2_03_router_hierarchy_middlewares:app
"""

from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware
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


@parent_router.subscriber(
    "parent-subscriber",
    topic_name="some_test_topic2",
    subscription_name="tst_sub",
)
async def parent_router_handle(_: Message) -> None:
    logger.info("We received a message!")


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
    await parent_router.publish(topic_name="some_test_topic2", data={"C": "D"})
    await child_router.publish(topic_name="some_test_topic3", data={"E": "F"})
