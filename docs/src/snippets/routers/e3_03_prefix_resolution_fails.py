"""Title: Cross-Level Alias Conflict Detection

Demonstrates the error when a router and broker have subscribers with the same alias.

This example shows:
- Creating an unnamed router (empty prefix) and a broker
- Using the same alias on both the router and the broker
- FastPubSub detects this cross-level conflict and raises an error

This is expected to fail! Even when subscribers are at "different" levels
(broker vs router), they must have unique aliases when the router has no prefix.

Run with:
    fastpubsub run examples.routers.e3_03_prefix_resolution_fails:app

Expected behavior:
    This will raise an error due to duplicate alias 'test-alias-abc'

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

first_unnamed_router = PubSubRouter(prefix="")

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router=first_unnamed_router)

app = FastPubSub(broker)


@first_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_first_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on first unnamed router: {message}")


# Should fail since the two subscriber are resolved as "test-alias-abc"
# Even if they are at "different" levels.
@broker.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on second unnamed router: {message}")
