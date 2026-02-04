from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

# --8<-- [start:cross_project_config]
PROJECT_ID = "fastpubsub-pubsub-local"
ALTERNATIVE_PROJECT_ID = "fastpubsub-pubsub-alternative"

broker = PubSubBroker(project_id=PROJECT_ID)
app = FastPubSub(broker)
# --8<-- [end:cross_project_config]


# --8<-- [start:cross_project_subscriber]
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


# --8<-- [end:cross_project_subscriber]


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", "hi!")
    await broker.publish("test-topic", "hi!", project_id=ALTERNATIVE_PROJECT_ID)

