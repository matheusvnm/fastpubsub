from examples.middlewares.middlewares import BrokerMiddleware, RouterMiddleware
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

router = PubSubRouter(prefix="core", middlewares=[RouterMiddleware])
broker = PubSubBroker(project_id="fastpubsub-pubsub-local", middlewares=[BrokerMiddleware], routers=[router])
app = FastPubSub(broker)


@router._add_subscriber("router-subscriber", topic_name="topic_b", subscription_name="subscription_b",)
async def router_handle(message: Message):
    logger.info(f"We received message {message} on router_handle")

@broker._add_subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"We received message {message} on broker_handle")


@app.after_startup
async def after_started():
    await router.publish(topic_name="topic_b", data={"some_message": "messageA"})
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})
