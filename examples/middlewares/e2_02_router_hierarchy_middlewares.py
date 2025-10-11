from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware
from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

child_router = PubSubRouter(prefix="subrouter")
child_router.include_middleware(SubRouterMiddleware)

parent_router = PubSubRouter(prefix="router")
parent_router.include_middleware(RouterMiddleware)
parent_router.include_router(child_router)

broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
)
broker.include_middleware(BrokerMiddleware)
broker.include_router(parent_router)

app = FastPubSub(broker)


@broker.subscriber(
    "broker-subscriber",
    topic_name="some_test_topic",
    subscription_name="tst_sub",
)
async def broker_handle(_: Message) -> None:
    logger.info("We received a message on broker!")


@parent_router.subscriber(
    "parent-subscriber",
    topic_name="some_test_topic2",
    subscription_name="tst_sub",
)
async def parent_router_handle(_: Message) -> None:
    logger.info("We received a message on parent router!")


@child_router.subscriber(
    "child-subscriber",
    topic_name="some_test_topic3",
    subscription_name="tst_sub",
)
async def subrouter_handle(_: Message) -> None:
    logger.info("We received a message on subrouter!")


@app.after_startup
async def after_started() -> None:
    await broker.publish(topic_name="some_test_topic", data={"A": "B"})
    await parent_router.publish(topic_name="some_test_topic2", data={"C": "D"})
    await child_router.publish(topic_name="some_test_topic3", data={"E": "F"})
