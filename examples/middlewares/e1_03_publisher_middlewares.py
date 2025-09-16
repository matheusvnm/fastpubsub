




from typing import Any


from examples.middlewares.middlewares import PublisherMiddleware, RouterMiddleware
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.routing.router import PubSubRouter

router = PubSubRouter(prefix="core", middlewares=[RouterMiddleware])
broker = PubSubBroker(project_id="fastpubsub-pubsub-local", routers=[router])
app = FastPubSub(broker)

@router.subscriber("router-subscriber", topic_name="topic_b", subscription_name="subscription_b",)
async def router_handle(message: Message):
    logger.info(f"We received message {message} on router_handle")

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"We received message {message} on broker_handle")

publisher = broker.publisher("topic_a")
publisher.include_middleware(PublisherMiddleware)

@app.after_startup
async def test_publish():
    await router.publish(topic_name="topic_b", data={"some_message": "messageB"})
    await publisher.publish(data={"some_message": "messageA"})

