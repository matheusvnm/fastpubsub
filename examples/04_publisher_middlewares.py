



import asyncio
from typing import Any

import uvicorn
from fastpubsub.applications import Application, FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares import BasePublisherMiddleware
from fastpubsub.router import PubSubRouter

class BrokerLevelPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"This is a global publisher middleware for sending messages")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)

class RouterLevelPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"This is a router-level publisher middleware for sending messages")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


router = PubSubRouter(middlewares=[RouterLevelPublisherMiddleware])

@router.subscriber("router-subscriber", topic_name="topic_b", subscription_name="subscription_b",)
async def router_handle(message: Message):
    logger.info(f"We received message {message} on router_handle")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local", middlewares=[BrokerLevelPublisherMiddleware], routers=[router])

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"We received message {message} on broker_handle")


app = FastPubSub(broker)


@app.after_startup
async def after_started():
    logger.info("The next published message will have one middleware")
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})

    logger.info("The next published message will have two middlewares")
    await router.publish(topic_name="topic_f", data={"some_message": "messageA"})