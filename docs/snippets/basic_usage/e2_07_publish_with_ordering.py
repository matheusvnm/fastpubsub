from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "user-events-handler",
    topic_name="user-events",
    subscription_name="user-events-subscription",
    enable_message_ordering=True,
)
async def handle_user_event(message: Message) -> None:
    logger.info(f"Received: {message.data.decode()}")
    logger.info(f"Ordering key: {message.ordering_key}")


# --8<-- [start:publish_ordering_broker]
@app.after_startup
async def publish_with_broker() -> None:
    await broker.publish(
        topic_name="user-events",
        data={"action": "login", "user_id": "user-123"},
        ordering_key="user-123",  # Same key ensures order
    )

    await broker.publish(
        topic_name="user-events",
        data={"action": "update_profile", "user_id": "user-123"},
        ordering_key="user-123",  # Same key ensures order
    )


# --8<-- [end:publish_ordering_broker]


# --8<-- [start:publish_ordering_publisher]
ordered_publisher: Publisher = broker.publisher("user-events")


@app.after_startup
async def publish_with_publisher() -> None:
    # Publish with ordering key
    await ordered_publisher.publish(
        data={"action": "login", "user_id": "user-123"},
        ordering_key="user-123",  # Same key ensures order
    )

    await ordered_publisher.publish(
        data={"action": "update_profile", "user_id": "user-123"},
        ordering_key="user-123",  # Same key ensures order
    )


# --8<-- [end:publish_ordering_publisher]
