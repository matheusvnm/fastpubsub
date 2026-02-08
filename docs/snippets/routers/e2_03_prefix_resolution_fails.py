"""Title: Duplicate Alias Conflict Detection

Demonstrates the error when two routers with empty prefixes have the same alias.

This example shows:
- Creating two routers with empty prefixes
- Using the same alias on both routers
- FastPubSub detects this conflict and raises an error

This is expected to fail! It demonstrates that FastPubSub properly validates
subscriber uniqueness and prevents accidental alias collisions.

Run with:
    fastpubsub run examples.routers.e2_03_prefix_resolution_fails:app

Expected behavior:
    This will raise an error due to duplicate alias 'test-alias-abc'

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:duplicate_alias_error]
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

first_unnamed_router = PubSubRouter(prefix="")
second_unnamed_router = PubSubRouter(prefix="")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router=first_unnamed_router)
broker.include_router(router=second_unnamed_router)

app = FastPubSub(broker)


@first_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_first_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on first unnamed router: {message}")


# Should fail since the two subscriber are resolved as "test-alias-abc"
@second_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on second unnamed router: {message}")


# --8<-- [end:duplicate_alias_error]
