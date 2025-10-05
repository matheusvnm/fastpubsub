from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

child_router = PubSubRouter(prefix="subrouter", middlewares=[SubRouterMiddleware])
parent_router = PubSubRouter(
    prefix="router", routers=[child_router], middlewares=[RouterMiddleware]
)
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local", middlewares=[BrokerMiddleware], routers=[parent_router]
)
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
