



import asyncio

import uvicorn
from fastpubsub.applications import Application, FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares import BaseSubscriberMiddleware
from fastpubsub.router import PubSubRouter


class BrokerLevelSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"This is a global subscriber middleware with message: {message.data}")
        return await super().__call__(message)

class RouterLevelSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"This is a router-level subscriber middleware with message: {message.data}")
        return await super().__call__(message)


class SubscriberLevelSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"This is a subscriber-level subscriber middleware with message: {message.data}")
        return await super().__call__(message)


router = PubSubRouter(middlewares=[RouterLevelSubscriberMiddleware])

@router.subscriber("router-subscriber", topic_name="topic_b", subscription_name="subscription_b",)
async def router_handle(message: Message):
    logger.info(f"This handler has a router and broker middlewares")


@router.subscriber("router-subscriber-with-mid", topic_name="topic_c", subscription_name="subscription_c", middlewares=[SubscriberLevelSubscriberMiddleware])
async def router_handle_with_middleware(message: Message):
    logger.info(f"This handler has all middlewares")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local", middlewares=[BrokerLevelSubscriberMiddleware], routers=[router])

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"This handler has only the broker middleware")


app = FastPubSub(broker)


@app.after_startup
async def after_started():
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})
    await broker.publish(topic_name="topic_b", data={"some_message": "messageA"})
    await broker.publish(topic_name="topic_c", data={"some_message": "messageA"})