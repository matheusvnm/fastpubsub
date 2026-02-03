"""Title: Dead-Letter Topic Basic Configuration

Demonstrates basic dead-letter topic configuration in FastPubSub.
"""

from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


# --8<-- [start:basic_config]
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
# --8<-- [end:basic_config]


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
async def call_external_api(message: Message):
    await external_api.call(message.data)
# --8<-- [end:backoff_config]


async def process_payment(data: bytes) -> None:
    """Placeholder for payment processing."""
    pass


class external_api:
    @staticmethod
    async def call(data: bytes) -> None:
        """Placeholder for external API call."""
        pass
