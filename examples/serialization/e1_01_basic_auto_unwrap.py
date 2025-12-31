"""Example: Basic auto-unwrapping of message data.

This example demonstrates how FastPubSub automatically unwraps
JSON message data into handler parameters.

When you publish a JSON message like {"user_id": "123", "action": "click"},
FastPubSub can automatically extract these fields and pass them to your
handler as parameters.
"""

from fastpubsub import FastPubSub, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# Handler with auto-unwrapping: parameters are extracted from the JSON message
@broker.subscriber(
    "auto-unwrap-example",
    topic_name="events-topic",
    subscription_name="events-subscription",
)
async def handle_event(user_id: str, action: str) -> None:
    """Handle event with auto-unwrapped parameters.

    When a message like {"user_id": "123", "action": "click"} is published,
    FastPubSub automatically extracts user_id and action from the JSON.
    """
    logger.info(f"User {user_id} performed action: {action}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish(
        "events-topic",
        {
            "user_id": "user-123",
            "action": "purchase",
            "extra_field": "ignored",  # Fields not in handler signature are ignored
        },
    )
