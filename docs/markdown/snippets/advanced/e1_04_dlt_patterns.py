"""Title: Dead-Letter Topic Common Patterns

Demonstrates common patterns for handling dead-letter messages.
"""

from datetime import datetime

from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


# --8<-- [start:alert_and_store]
@broker.subscriber(
    alias="dlq-alert-store",
    topic_name="orders-dlq",
    subscription_name="orders-dlq-subscription",
)
async def handle_dlq(message: Message):
    await slack_webhook.send(f"Failed message: {message.id}")
    await database.insert(
        "failed_messages",
        {
            "message_id": message.id,
            "data": message.data,
            "failed_at": datetime.utcnow(),
        },
    )
# --8<-- [end:alert_and_store]


# --8<-- [start:retry_fallback]
@broker.subscriber(
    alias="dlq-retry",
    topic_name="payments-dlq",
    subscription_name="payments-dlq-subscription",
)
async def retry_with_fallback(message: Message):
    await fallback_payment_service.process(message.data)
# --8<-- [end:retry_fallback]


# --8<-- [start:manual_review]
@broker.subscriber(
    alias="dlq-review",
    topic_name="orders-dlq",
    subscription_name="orders-dlq-subscription",
)
async def queue_for_review(message: Message):
    await admin_dashboard.create_ticket(
        title=f"Failed order: {message.id}",
        data=message.data,
        priority="high",
    )
# --8<-- [end:manual_review]


# Placeholder classes
class slack_webhook:
    @staticmethod
    async def send(msg: str) -> None:
        pass


class database:
    @staticmethod
    async def insert(table: str, data: dict) -> None:
        pass


class fallback_payment_service:
    @staticmethod
    async def process(data: bytes) -> None:
        pass


class admin_dashboard:
    @staticmethod
    async def create_ticket(**kwargs) -> None:
        pass
