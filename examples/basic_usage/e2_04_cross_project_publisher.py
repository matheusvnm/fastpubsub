"""Example: Cross-project publishing.

This example demonstrates publishing messages to topics in different
Google Cloud projects. Each publisher can specify its own project_id,
overriding the broker's default project. This enables communication
across multiple GCP projects from a single application.

Flow:
1. Message arrives at first project's topic
2. Handler publishes to second project's topic
3. Handler publishes to third project's topic via explicit Publisher

Run with: fastpubsub run examples.basic_usage.e2_04_cross_project_publisher:app
"""

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

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
