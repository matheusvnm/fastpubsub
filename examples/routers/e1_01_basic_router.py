"""Example: Basic router usage.

This example demonstrates how to organize subscribers using routers.
Routers provide a way to group related subscribers and apply common
configuration (like prefixes) to them.

The prefix allows multiple subscribers to use the same alias without
conflict, as the final identifier becomes "prefix-alias".

Run with: fastpubsub run examples.routers.e1_01_basic_router:app
"""

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

router = PubSubRouter(prefix="core")
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")

broker.include_router(router=router)
app = FastPubSub(broker)


@router.subscriber(
    "test-alias",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handler_on_router(message: Message) -> None:
    logger.info(f"Processed message on router handler: {message}")


# The aliases/subscription name can be the same.
# That is because the PubSubRouter has prefix.
@broker.subscriber(
    "test-alias",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handler_on_broker(message: Message) -> None:
    logger.info(f"Processed message on broker handler: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-router-topic", {"hello": "world"})
