"""Title: Dead-Letter Topic Monitoring

Demonstrates how to monitor and handle messages in a dead-letter topic.

This example shows:
- Setting up a subscriber with dead-letter topic configuration
- Creating a handler for the dead-letter topic
- Logging failed messages with context
- Alerting and storing failed messages for later analysis

Dead-letter topics (DLT) catch messages that fail processing after multiple
retry attempts. Monitoring your DLT is essential for identifying and fixing
issues with your message handlers.

Run with:
    fastpubsub run examples.advanced.e1_01_dlt_monitoring:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

import json
from datetime import datetime

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Retry
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-local")
app = FastPubSub(broker)


# Storage for failed messages (in production, use a database)
failed_messages: list[dict] = []


@broker.subscriber(
    alias="order-processor",
    topic_name="orders",
    subscription_name="orders-subscription",
    dead_letter_topic="orders-dlq",
    max_delivery_attempts=5,
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=60,
    autocreate=True,
)
async def process_order(message: Message) -> None:
    """Process order messages. Failed messages go to DLT after 5 attempts."""
    try:
        order_data = json.loads(message.data)
        logger.info(f"Processing order: {order_data.get('order_id')}")

        # Simulate processing that might fail
        if order_data.get("invalid"):
            raise ValueError("Invalid order data")

        logger.info(f"Order {order_data.get('order_id')} processed successfully")

    except Exception as e:
        logger.warning(f"Order processing failed (attempt {message.delivery_attempt}): {e}")
        raise Retry(f"Processing failed: {e}")


@broker.subscriber(
    alias="dlq-handler",
    topic_name="orders-dlq",
    subscription_name="orders-dlq-subscription",
    autocreate=True,
)
async def handle_failed_orders(message: Message) -> None:
    """Handle messages that failed processing and ended up in the DLT."""
    # Log the failure with context
    logger.error(
        f"Message {message.id} failed permanently after {message.delivery_attempt} attempts",
        extra={
            "message_id": message.id,
            "message_data": message.data.decode("utf-8"),
            "attributes": message.attributes,
            "topic": message.topic_name,
        },
    )

    # Store for later analysis
    failed_record = {
        "message_id": message.id,
        "data": message.data.decode("utf-8"),
        "attributes": message.attributes,
        "failed_at": datetime.utcnow().isoformat(),
        "delivery_attempts": message.delivery_attempt,
    }
    failed_messages.append(failed_record)

    # In production, you would:
    # - Send an alert to your ops team
    # - Store in a database for review
    # - Create a support ticket
    logger.info(f"Stored failed message for review. Total failed: {len(failed_messages)}")


@app.after_startup
async def test_dlt() -> None:
    """Publish test messages to demonstrate DLT behavior."""
    # This message will succeed
    await broker.publish(
        "orders",
        {"order_id": "12345", "amount": 99.99},
    )

    # This message will fail and eventually go to DLT
    await broker.publish(
        "orders",
        {"order_id": "99999", "invalid": True},
    )

    logger.info("Published test messages - watch for DLT activity")
