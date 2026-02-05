"""Title: Basic Router with Topic Prefixes

Demonstrates basic router usage with prefixes to organize subscribers.

This example shows:
- Creating a PubSubRouter with a prefix
- Including a router in a broker with include_router()
- How the same alias and subscription names can be used on both router and broker
  because the router's prefix makes them unique

The prefix mechanism allows organizing subscribers into logical groups while
preventing naming conflicts.

Run with:
    fastpubsub run examples.routers.e1_01_basic_router:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:basic_router_full]
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

# --8<-- [start:router_setup]
router = PubSubRouter(prefix="core")
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")

broker.include_router(router=router)
app = FastPubSub(broker)
# --8<-- [end:router_setup]


# --8<-- [start:router_subscriber]
@router.subscriber(
    "test-alias",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handler_on_router(message: Message) -> None:
    logger.info(f"Processed message on router handler: {message}")


# --8<-- [end:router_subscriber]


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


# --8<-- [end:basic_router_full]
