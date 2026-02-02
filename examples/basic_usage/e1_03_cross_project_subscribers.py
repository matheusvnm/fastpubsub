"""Title: Cross-Project Subscription

Demonstrates how to subscribe to topics in different Google Cloud projects.

This example shows:
- Creating subscribers that listen to topics in different GCP projects
- Using the project_id parameter on @broker.subscriber to override the default project
- Publishing messages to topics in different projects

The example creates two subscribers:
1. One listening to the default project's topic
2. Another listening to the same topic name but in an alternative project

Run with:
    fastpubsub run examples.basic_usage.e1_03_cross_project_subscribers:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
    - Ensure both projects exist and are accessible (Only for GCP on Cloud)
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

PROJECT_ID = "fastpubsub-pubsub-local"
ALTERNATIVE_PROJECT_ID = "fastpubsub-pubsub-alternative"

broker = PubSubBroker(project_id=PROJECT_ID)
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias-001",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
)
async def process_message(message: Message) -> None:
    logger.info(f"Processed message for the project {PROJECT_ID}")


@broker.subscriber(
    "test-alias-002",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
    project_id=ALTERNATIVE_PROJECT_ID,
)
async def process_message_alternative_project(message: Message) -> None:
    logger.info(f"Processed message for the project {ALTERNATIVE_PROJECT_ID}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", "hi!")
    await broker.publish("test-topic", "hi!", project_id=ALTERNATIVE_PROJECT_ID)
