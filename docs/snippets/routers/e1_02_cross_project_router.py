"""Title: Router Bound to Different Project

Demonstrates creating a router that operates on a different GCP project than the broker.

This example shows:
- Creating a router with a specific project_id different from the broker
- All subscribers/publishers on that router default to the router's project
- Cross-project message flow between broker and router
- Using a publisher to send messages to a different project

This is useful for multi-project architectures where different parts of your
application communicate across GCP project boundaries.

Run with:
    fastpubsub run examples.routers.e1_02_cross_project_router:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
    - Ensure both projects exist and are accessible
"""

# --8<-- [start:cross_project_router_full]
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

PROJECT_ID = "fastpubsub-pubsub-local"
ALTERNATIVE_PROJECT_ID = "fastpubsub-pubsub-alternative"

TOPIC_NAME = "test-router-topic-mult-project"
SUBSCRIPTION_NAME = "test-basic-router-subscription"

# --8<-- [start:router_with_project]
# The router will be attached to a specific project
# All the subscribers/publishers created by this router will point to this project.
# Unless, you set the subscriber/publisher to another project themselves.
router = PubSubRouter(prefix="core", project_id=ALTERNATIVE_PROJECT_ID)
broker = PubSubBroker(project_id=PROJECT_ID)

alternative_project_publisher = broker.publisher(
    topic_name=TOPIC_NAME, project_id=ALTERNATIVE_PROJECT_ID
)

broker.include_router(router=router)
# --8<-- [end:router_with_project]
app = FastPubSub(broker)


@router.subscriber(
    "test-alias",
    topic_name=TOPIC_NAME,
    subscription_name=SUBSCRIPTION_NAME,
)
async def handler_on_router(message: Message) -> None:
    logger.info(f"Processed message on router handler on project {ALTERNATIVE_PROJECT_ID}")


# The aliases/subscription name can be the same.
# That is because the PubSubRouter has prefix.
# Also, the topic and subscriptions can be the same due to them being on different projects.
@broker.subscriber(
    "test-alias",
    topic_name=TOPIC_NAME,
    subscription_name=SUBSCRIPTION_NAME,
)
async def handler_on_broker(message: Message) -> None:
    logger.info(f"Processed message on broker handler on project {PROJECT_ID}")
    await alternative_project_publisher.publish(b"some_message")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-router-topic-mult-project", {"hello": "world"})


# --8<-- [end:cross_project_router_full]
