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
