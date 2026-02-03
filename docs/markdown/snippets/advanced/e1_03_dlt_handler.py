"""Title: Dead-Letter Topic Handler

Demonstrates how to handle messages that arrive in a dead-letter topic.
"""

import logging

from fastpubsub import FastPubSub, Message, PubSubBroker

logger = logging.getLogger(__name__)

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


# --8<-- [start:dlt_handler]
@broker.subscriber(
    alias="dlq-handler",
    topic_name="orders-dlq",
    subscription_name="orders-dlq-subscription",
    autocreate=True,
)
async def handle_failed_orders(message: Message):
    logger.error(
        f"Message {message.id} failed permanently",
        extra={
            "message_data": message.data.decode("utf-8"),
            "attributes": message.attributes,
            "delivery_attempt": message.delivery_attempt,
        },
    )
    await send_alert_to_ops_team(message)
    await store_failed_message(message)
# --8<-- [end:dlt_handler]


async def send_alert_to_ops_team(message: Message) -> None:
    """Placeholder for alerting."""
    pass


async def store_failed_message(message: Message) -> None:
    """Placeholder for storage."""
    pass
