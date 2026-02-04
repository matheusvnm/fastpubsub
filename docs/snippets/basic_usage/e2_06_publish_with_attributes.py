"""Title: Publishing with Attributes

Demonstrates publishing messages with custom attributes for metadata.

This example shows:
- Adding attributes to messages via broker.publish()
- Adding attributes via Publisher object
- Using attributes for server-side filtering
- Adding routing and context information to messages

Attributes are key-value string pairs that appear in the message metadata,
separate from the message payload. Useful for filtering and routing.

Run with:
    fastpubsub run docs.snippets.basic_usage.e2_06_publish_with_attributes:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:publish_attributes_full]
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
# --8<-- [end:publish_attributes_full]
