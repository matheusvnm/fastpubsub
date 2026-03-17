from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "events-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_event(message: Message) -> None:
    logger.info(f"Received: {message.data.decode()}")
    logger.info(f"Attributes: {message.attributes}")


# --8<-- [start:publish_attributes_broker]
@app.after_startup
async def publish_with_broker() -> None:
    await broker.publish(
        topic_name="events",
        data={"user_id": "123", "action": "login"},
        attributes={"event_type": "user_login", "priority": "high"},
    )


# --8<-- [end:publish_attributes_broker]


# --8<-- [start:publish_attributes_publisher]
event_publisher: Publisher = broker.publisher("events")


@app.after_startup
async def publish_with_publisher() -> None:
    await event_publisher.publish(
        data={"user_id": "321", "action": "login"},
        attributes={"event_type": "user_login", "priority": "high"},
    )


# --8<-- [end:publish_attributes_publisher]
