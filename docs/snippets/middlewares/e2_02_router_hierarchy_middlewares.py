from middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubRouterMiddleware

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

# --8<-- [start:include_middleware_pattern]
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
# --8<-- [end:include_middleware_pattern]

app = FastPubSub(broker)


@broker.subscriber(
    "broker-subscriber",
    topic_name="some_test_topic",
    subscription_name="tst_sub",
)
async def broker_handle(_: Message) -> None:
    logger.info("We received a message on broker!")
    await parent_router.publish(topic_name="some_test_topic2", data={"C": "D"})


@parent_router.subscriber(
    "parent-subscriber",
    topic_name="some_test_topic2",
    subscription_name="tst_sub",
)
async def parent_router_handle(_: Message) -> None:
    logger.info("We received a message on parent router!")
    await child_router.publish(topic_name="some_test_topic3", data={"E": "F"})


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
