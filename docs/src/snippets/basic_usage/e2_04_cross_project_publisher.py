"""Title: Cross-Project Publishing

Demonstrates publishing messages across multiple Google Cloud projects.

This example shows:
- Creating publishers and subscribers across three different GCP projects
- Using project_id parameter on broker.publisher() to target different projects
- Message flow across projects: first -> second -> third
- Chaining message processing across project boundaries

The flow:
1. Message published to first project triggers subscriber
2. First subscriber publishes to second project
3. Second subscriber publishes to third project using a Publisher instance

Run with:
    fastpubsub run examples.basic_usage.e2_04_cross_project_publisher:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
    - Ensure all three projects exist and are accessible
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.pubsub import Publisher

DEFAULT_PROJECT_ID = "fastpubsub-pubsub-first-project"
SECOND_PROJECT_ID = "fastpubsub-pubsub-second-project"
THIRD_PROJECT_ID = "fastpubsub-pubsub-third-project"

broker = PubSubBroker(project_id=DEFAULT_PROJECT_ID)
app = FastPubSub(broker)

publisher: Publisher = broker.publisher("test-topic", project_id=THIRD_PROJECT_ID)


@broker.subscriber(
    "test-alias-001",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
)
async def process_message_first_project(message: Message) -> None:
    logger.info(f"Processed message for the project {DEFAULT_PROJECT_ID}")
    await broker.publish("test-topic", "hi!", project_id=SECOND_PROJECT_ID)


@broker.subscriber(
    "test-alias-002",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
    project_id=SECOND_PROJECT_ID,
)
async def process_message_second_project(message: Message) -> None:
    logger.info(f"Processed message for the project {SECOND_PROJECT_ID}")
    await publisher.publish({"hello": "world"})


@broker.subscriber(
    "test-alias-003",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
    project_id=THIRD_PROJECT_ID,
)
async def process_message_third_project(message: Message) -> None:
    logger.info(f"Processed message for the project {THIRD_PROJECT_ID}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", "hi!")
