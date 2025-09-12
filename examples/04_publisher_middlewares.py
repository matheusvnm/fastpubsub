



import asyncio
from typing import Any
from starconsumers.applications import StarConsumers
from starconsumers.broker import Broker
from starconsumers.datastructures import Message
from starconsumers.logger import logger
from starconsumers.middlewares import BasePublisherMiddleware
from starconsumers.router import Router

class BrokerLevelPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"This is a global publisher middleware for sending messages")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)

class RouterLevelPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"This is a router-level publisher middleware for sending messages")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


router = Router(middlewares=[RouterLevelPublisherMiddleware])

@router.subscriber("router-subscriber", topic_name="topic_b", subscription_name="subscription_b",)
async def router_handle(message: Message):
    logger.info(f"We received message {message} on router_handle")


broker = Broker(project_id="starconsumers-pubsub-local", middlewares=[BrokerLevelPublisherMiddleware], routers=[router])

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"We received message {message} on broker_handle")


app = StarConsumers(broker=broker)


@app.after_startup
async def after_started():
    logger.info("The next published message will have one middleware")
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})

    logger.info("The next published message will have two middlewares")
    await router.publish(topic_name="topic_f", data={"some_message": "messageA"})



if __name__ == "__main__":
    asyncio.run(app.run())
