"""Example: Nested routers.

This example shows how to nest routers within other routers,
creating a hierarchy for organizing complex applications. Each
router has its own prefix, allowing the same alias to be used
across different routers without conflicts.

Structure: broker -> core -> (sales, logistics)

Run with: fastpubsub run examples.routers.e1_02_nested_routers:app
"""

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

# The aliases/subscription name can be the same.
# That is because each PubSubRouter has prefix.
# Hence, their message handler do not conflict
router_core = PubSubRouter(prefix="core")
router_sales = PubSubRouter(prefix="sales")
router_logistics = PubSubRouter(prefix="logistics")

router_core.include_router(router_sales)
router_core.include_router(router_logistics)


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router_core)


app = FastPubSub(broker)


@router_core.subscriber(
    "some-alias",
    topic_name="some-router-topic",
    subscription_name="some-router-sub",
)
async def handler_on_core_router(message: Message) -> None:
    logger.info(f"Processed message on core router: {message}")


@router_sales.subscriber(
    "some-alias",
    topic_name="some-router-topic",
    subscription_name="some-router-sub",
)
async def handler_on_sales_router(message: Message) -> None:
    logger.info(f"Processed message on sales router: {message}")


@router_logistics.subscriber(
    "some-alias",
    topic_name="some-router-topic",
    subscription_name="some-router-sub",
)
async def handler_on_logistics_router(message: Message) -> None:
    logger.info(f"Processed message on logistics handler: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("some-router-topic", {"hello": "world"})
