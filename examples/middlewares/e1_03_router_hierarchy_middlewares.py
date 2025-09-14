



import asyncio
from typing import Any

import uvicorn
from fastpubsub.applications import Application, FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from fastpubsub.routing.router import PubSubRouter

class BrokerLevelSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the broker subscriber middleware! I will only be executed at broker handlers")
        return await super().__call__(message)

class BrokerLevelPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the broker publisher middleware! I will only be executed at broker publishers")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)

class ParentRouterSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the parent router subscriber middleware! I will only be executed at the parent router handlers and its children")
        return await super().__call__(message)

class ParentRouterPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the parent router publisher middleware! I will only be executed at the parent router publishers and its children")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


class ChildRouterSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the child router subscriber middleware! I will only be executed at the child router handlers")
        return await super().__call__(message)


class ChildRouterPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the child router publisher middleware! I will only be executed at the child router publishers")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


child_router = PubSubRouter(prefix="child", middlewares=[ChildRouterSubscriberMiddleware, ChildRouterPublisherMiddleware])
parent_router = PubSubRouter(prefix="parent", routers=[child_router], middlewares=[ParentRouterSubscriberMiddleware, ParentRouterPublisherMiddleware])
broker = PubSubBroker(project_id="fastpubsub-pubsub-local", middlewares=[BrokerLevelSubscriberMiddleware, BrokerLevelPublisherMiddleware], routers=[parent_router])
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
