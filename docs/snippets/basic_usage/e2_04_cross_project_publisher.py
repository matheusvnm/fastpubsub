from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.pubsub import Publisher

# --8<-- [start:cross_project_setup]
DEFAULT_PROJECT_ID = "fastpubsub-pubsub-first-project"
SECOND_PROJECT_ID = "fastpubsub-pubsub-second-project"
THIRD_PROJECT_ID = "fastpubsub-pubsub-third-project"

broker = PubSubBroker(project_id=DEFAULT_PROJECT_ID)
app = FastPubSub(broker)

publisher: Publisher = broker.publisher("test-topic", project_id=THIRD_PROJECT_ID)
# --8<-- [end:cross_project_setup]


# --8<-- [start:cross_project_subscribers]
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


# --8<-- [end:cross_project_subscribers]

@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", "hi!")