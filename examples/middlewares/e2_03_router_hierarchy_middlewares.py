

from examples.middlewares.middlewares import BrokerPublisherMiddleware, BrokerSubscriberMiddleware, RouterPublisherMiddleware, RouterSubscriberMiddleware, SubRouterPublisherMiddleware, SubRouterSubscriberMiddleware
from fastpubsub.applications import  FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.routing.router import PubSubRouter


# It works in any order!!!
child_router = PubSubRouter(prefix="subrouter")
parent_router = PubSubRouter(prefix="router")
broker = PubSubBroker(project_id="fastpubsub-pubsub-local",)
broker.include_router(parent_router)
parent_router.include_router(child_router)

child_router.include_middleware(SubRouterPublisherMiddleware)
child_router.include_middleware(SubRouterSubscriberMiddleware)

parent_router.include_middleware(RouterPublisherMiddleware)
parent_router.include_middleware(RouterSubscriberMiddleware)

broker.include_middleware(BrokerSubscriberMiddleware)
broker.include_middleware(BrokerPublisherMiddleware)


app = FastPubSub(broker)

@broker.subscriber("broker-subscriber", 
                   topic_name="some_test_topic", 
                   subscription_name="tst_sub",)
async def broker_handle(message: Message):
    logger.info("We received a message!")


@parent_router.subscriber("parent-subscriber", 
                   topic_name="some_test_topic2", 
                   subscription_name="tst_sub",)
async def parent_router_handle(message: Message):
    logger.info("We received a message!")


@child_router.subscriber("child-subscriber", 
                   topic_name="some_test_topic3", 
                   subscription_name="tst_sub",)
async def router_handle_with_middleware(message: Message):
    logger.info("We received a message!")


@app.after_startup
async def after_started():
    await broker.publish(topic_name="some_test_topic", data={"A": "B"})
    await parent_router.publish(topic_name="some_test_topic2", data={"C": "D"})
    await child_router.publish(topic_name="some_test_topic3", data={"E": "F"})
