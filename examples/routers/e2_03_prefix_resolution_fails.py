"""Example: Prefix resolution failure - duplicate aliases in unnamed routers.

This example demonstrates a conflict that occurs when two routers with
empty prefixes (prefix="") have subscribers with the same alias. Since
there's no prefix to differentiate them, both resolve to "test-alias-abc",
causing a conflict.

This will raise an error at startup, showing the importance of unique
aliases when not using prefixes.

Run with: fastpubsub run examples.routers.e2_03_prefix_resolution_fails:app
"""

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter

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
    pass


# Should fail since the two subscriber are resolved as "test-alias-abc"
@second_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    pass
