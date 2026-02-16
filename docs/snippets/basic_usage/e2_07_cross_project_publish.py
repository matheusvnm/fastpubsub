from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "shared-events-handler",
    topic_name="shared-events",
    subscription_name="shared-events-subscription",
    project_id="other-project-id",
)
async def handle_shared_event(message: Message) -> None:
    logger.info(f"Received cross-project message: {message.data.decode()}")


# --8<-- [start:cross_project_broker]
@app.after_startup
async def publish_cross_project_broker() -> None:
    await broker.publish(
        topic_name="shared-events",
        data={"event": "cross_project"},
        project_id="other-project-id",
    )


# --8<-- [end:cross_project_broker]


# --8<-- [start:cross_project_publisher]
cross_project_publisher: Publisher = broker.publisher(
    "shared-events",
    project_id="other-project-id",
)


async def publish_cross_project_publisher() -> None:
    await cross_project_publisher.publish(data={"event": "cross_project"})


# --8<-- [end:cross_project_publisher]
