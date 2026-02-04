"""Title: Prefix Resolution with Unique Aliases

Demonstrates that routers with empty prefixes can coexist when aliases are unique.

This example shows:
- Creating multiple routers with empty prefixes ("")
- Using different aliases for each subscriber to avoid conflicts
- How prefix resolution works when no prefix is specified

When using empty prefixes, you must ensure that all subscriber aliases are
globally unique, as there's no prefix to differentiate them.

Run with:
    fastpubsub run examples.routers.e1_03_prefix_resolution:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:prefix_resolution_full]
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

# --8<-- [start:empty_prefix_routers]
first_unnamed_router = PubSubRouter(prefix="")
second_unnamed_router = PubSubRouter(prefix="")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router=first_unnamed_router)
broker.include_router(router=second_unnamed_router)
# --8<-- [end:empty_prefix_routers]

app = FastPubSub(broker)


# --8<-- [start:unique_aliases]
@first_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_first_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on first unnamed router: {message}")


@second_unnamed_router.subscriber(
    "test-alias-cba",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on second unnamed router: {message}")


# --8<-- [end:unique_aliases]


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-router-topic", {"hello": "world"})


# --8<-- [end:prefix_resolution_full]
