"""Example: Prefix resolution failure - conflict between router and broker.

This example shows another conflict scenario where an unnamed router
(prefix="") and the broker itself have subscribers with the same alias.
Even though they are at "different" levels in the hierarchy, both resolve
to the same identifier "test-alias-abc".

This will raise an error at startup, demonstrating that alias uniqueness
must be maintained across all levels when using empty prefixes.

Run with: fastpubsub run examples.routers.e3_03_prefix_resolution_fails:app
"""

from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter

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
    pass


# Should fail since the two subscriber are resolved as "test-alias-abc"
# Even if they are at "different" levels.
@broker.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    pass
