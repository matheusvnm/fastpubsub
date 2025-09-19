from fastpubsub.applications import  FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware, SubcriberMiddleware


router = PubSubRouter(prefix="myawesomerouter", middlewares=[RouterMiddleware])
broker = PubSubBroker(project_id="fastpubsub-pubsub-local", middlewares=[BrokerMiddleware], routers=[router])
app = FastPubSub(broker)


@broker.subscriber("broker-subscriber",
                   topic_name="topic_one_mid",
                   subscription_name="subscription_one_mid",)
async def broker_handle(message: Message):
    logger.info("This handler has only the broker middleware")


@router.subscriber("router-subscriber",
                   topic_name="topic_two_mid",
                   subscription_name="subscription_two_mid",)
async def router_handle(message: Message):
    logger.info("This handler has a router and broker middlewares")


@router.subscriber("router-subscriber-with-mid",
                   topic_name="topic_three_mid",
                   subscription_name="subscription_three_mid",
                   middlewares=[SubcriberMiddleware])
async def router_handle_with_middleware(message: Message):
    logger.info("This handler has all middlewares")


@app.after_startup
async def after_started():
    await broker.publish(topic_name="topic_one_mid", data={"A": "B"})
    await broker.publish(topic_name="topic_two_mid", data={"C": "D"})
    await broker.publish(topic_name="topic_three_mid", data={"F": "G"})
