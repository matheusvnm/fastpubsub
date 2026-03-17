from datetime import UTC, datetime

import pytest

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def process_payment(data: bytes) -> None:
    """Process payment data."""
    pass


async def send_alert_to_ops_team(message: Message) -> None:
    """Send alert to operations team."""
    pass


async def store_failed_message(message: Message) -> None:
    """Store failed message for later analysis."""
    pass


# --8<-- [start:basic_dlt_config]
@broker.subscriber(
    alias="order-processor",
    topic_name="orders",
    subscription_name="orders-subscription",
    dead_letter_topic="orders-dlq",
    max_delivery_attempts=5,
    autocreate=True,
)
async def process_order(message: Message):
    await process_payment(message.data)


# --8<-- [end:basic_dlt_config]


async def call_external_api(data: bytes) -> None:
    """Call external API."""
    pass


# --8<-- [start:backoff_config]
@broker.subscriber(
    alias="api-caller",
    topic_name="api-requests",
    subscription_name="api-requests-subscription",
    dead_letter_topic="api-requests-dlq",
    max_delivery_attempts=10,
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=600,
    autocreate=True,
)
async def call_api(message: Message):
    await call_external_api(message.data)


# --8<-- [end:backoff_config]


# --8<-- [start:dlq_handler]
@broker.subscriber(
    alias="dlq-handler",
    topic_name="orders-dlq",
    subscription_name="orders-dlq-subscription",
    autocreate=True,
)
async def handle_failed_orders(message: Message):
    # Log the failure with details
    logger.error(
        f"Message {message.id} failed permanently",
        extra={
            "message_data": message.data.decode("utf-8"),
            "attributes": message.attributes,
            "delivery_attempt": message.delivery_attempt,
        },
    )

    # Alert your operations team
    await send_alert_to_ops_team(message)

    # Store for later analysis
    await store_failed_message(message)


# --8<-- [end:dlq_handler]


# Simulated services for patterns
class SlackWebhook:
    async def send(self, message: str) -> None:
        pass


class Database:
    async def insert(self, table: str, data: dict) -> None:
        pass


class FallbackPaymentService:
    async def process(self, data: bytes) -> None:
        pass


class AdminDashboard:
    async def create_ticket(
        self, title: str, data: bytes, priority: str
    ) -> None:
        pass


slack_webhook = SlackWebhook()
database = Database()
fallback_payment_service = FallbackPaymentService()
admin_dashboard = AdminDashboard()


# --8<-- [start:dlq_pattern_alert_store]
@broker.subscriber(
    alias="dlq-alert-store",
    topic_name="events-dlq",
    subscription_name="events-dlq-subscription",
)
async def handle_dlq_alert_store(message: Message):
    await slack_webhook.send(f"Failed message: {message.id}")
    await database.insert(
        "failed_messages",
        {
            "message_id": message.id,
            "data": message.data,
            "failed_at": datetime.now(UTC),
        },
    )


# --8<-- [end:dlq_pattern_alert_store]


# --8<-- [start:dlq_pattern_retry]
@broker.subscriber(
    alias="dlq-retry",
    topic_name="payments-dlq",
    subscription_name="payments-dlq-subscription",
)
async def retry_with_fallback(message: Message):
    # Try a fallback payment processor
    await fallback_payment_service.process(message.data)


# --8<-- [end:dlq_pattern_retry]


# --8<-- [start:dlq_pattern_review]
@broker.subscriber(
    alias="dlq-review",
    topic_name="tickets-dlq",
    subscription_name="tickets-dlq-subscription",
)
async def queue_for_review(message: Message):
    await admin_dashboard.create_ticket(
        title=f"Failed order: {message.id}",
        data=message.data,
        priority="high",
    )


# --8<-- [end:dlq_pattern_review]


# --8<-- [start:dlt_test_client]
@pytest.mark.asyncio
async def test_failed_message_reaches_error_result_stream() -> None:
    test_broker = PubSubBroker(project_id="test-project")

    @test_broker.subscriber(
        alias="always-fails",
        topic_name="orders",
        subscription_name="orders-subscription",
        dead_letter_topic="orders-dlq",
        max_delivery_attempts=5,
    )
    async def always_fails(_: Message) -> None:
        raise ValueError("invalid payload")

    async with PubSubTestClient(test_broker) as client:
        await client.publish(topic="orders", data={"order_id": "ord-1"})
        results = client.get_results()

    assert len(results) == 1
    assert isinstance(results[0].error, ValueError)


# --8<-- [end:dlt_test_client]
