"""Title: Nested Router Hierarchy

Demonstrates creating nested routers for organizing complex applications.

This example shows:
- Creating multiple routers with different prefixes (core, sales, logistics)
- Nesting routers using include_router()
- How the same alias/subscription names can be used across routers without conflict
- Each router's prefix ensures unique handler identification

This pattern is useful for organizing large applications into logical domains
or microservice-like boundaries while maintaining a single deployment.

Run with:
    fastpubsub run examples.routers.e1_02_nested_routers:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:nested_routers_full]
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

# --8<-- [start:nested_routers_setup]
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
# --8<-- [end:nested_routers_setup]


app = FastPubSub(broker)


# --8<-- [start:domain_subscribers]
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


# --8<-- [end:domain_subscribers]


@app.after_startup
async def test_publish() -> None:
    await broker.publish("some-router-topic", {"hello": "world"})


# --8<-- [end:nested_routers_full]
