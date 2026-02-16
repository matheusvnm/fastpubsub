from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.testing import PubSubTestClient

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def charge_customer(data: bytes) -> None:
    """Charge customer."""
    pass


async def process_event(data: bytes) -> None:
    """Process event."""
    pass


async def is_already_processed(event_id: str) -> bool:
    """Check if event is already processed."""
    return False


async def mark_as_processed(event_id: str) -> None:
    """Mark event as processed."""
    pass


# --8<-- [start:exactly_once]
@broker.subscriber(
    alias="payment-processor",
    topic_name="payments",
    subscription_name="payments-subscription",
    enable_exactly_once_delivery=True,
    autocreate=True,
)
async def process_payment(message: Message):
    # Guaranteed to run exactly once per message
    await charge_customer(message.data)


# --8<-- [end:exactly_once]


# --8<-- [start:idempotent_handler]
@broker.subscriber(
    alias="idempotent-handler",
    topic_name="events",
    subscription_name="events-subscription",
    # No exactly-once needed - handler is idempotent
)
async def idempotent_handler(message: Message):
    event_id = message.attributes.get("event_id")
    if not event_id:
        logger.warning("Missing event_id attribute, skipping message")
        return

    # Check if already processed
    if await is_already_processed(event_id):
        logger.info(f"Event {event_id} already processed, skipping")
        return

    await process_event(message.data)
    await mark_as_processed(event_id)


# --8<-- [end:idempotent_handler]


# Simulated database for idempotency patterns
class Database:
    async def execute(self, query: str, params: list) -> None:
        pass


class UniqueViolationError(Exception):
    pass


db = Database()


async def fulfill_order(data: bytes) -> None:
    """Fulfill order."""
    pass


# --8<-- [start:idempotent_database]
@broker.subscriber(
    alias="db-idempotent",
    topic_name="db-orders",
    subscription_name="db-orders-subscription",
)
async def db_idempotent_handler(message: Message):
    import json

    order_id = json.loads(message.data)["order_id"]

    # Use database transaction with unique constraint
    try:
        await db.execute("INSERT INTO processed_orders (order_id) VALUES (?)", [order_id])
    except UniqueViolationError:
        return  # Already processed

    await fulfill_order(message.data)


# --8<-- [end:idempotent_database]


# Simulated Redis for idempotency
class Redis:
    async def set(self, key: str, value: str, nx: bool = False, ex: int = 0) -> bool:
        return True


redis = Redis()


# --8<-- [start:idempotent_redis]
@broker.subscriber(
    alias="redis-idempotent",
    topic_name="redis-events",
    subscription_name="redis-events-subscription",
)
async def redis_idempotent_handler(message: Message):
    event_id = message.attributes.get("event_id")
    if not event_id:
        return

    # Set with NX (only if not exists), expire after 24 hours
    was_set = await redis.set(f"processed:{event_id}", "1", nx=True, ex=86400)

    if not was_set:
        return  # Already processed

    await process_event(message.data)


# --8<-- [end:idempotent_redis]


# --8<-- [start:exactly_once_combined]
@broker.subscriber(
    alias="critical-payment",
    topic_name="critical-payments",
    subscription_name="critical-payments-subscription",
    # Delivery guarantee
    enable_exactly_once_delivery=True,
    # Error handling
    dead_letter_topic="payments-dlq",
    max_delivery_attempts=5,
    # Backoff for transient failures
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=300,
    autocreate=True,
)
async def process_critical_payment(message: Message):
    await charge_customer(message.data)


# --8<-- [end:exactly_once_combined]


# --8<-- [start:idempotency_test_client]
async def test_idempotent_handler_skips_duplicates() -> None:
    test_broker = PubSubBroker(project_id="test-project")
    processed: list[str] = []

    @test_broker.subscriber(
        alias="orders",
        topic_name="orders",
        subscription_name="orders-subscription",
    )
    async def handle(message: Message) -> None:
        event_id = message.attributes.get("event_id", "")
        if event_id in processed:
            return
        processed.append(event_id)

    async with PubSubTestClient(test_broker) as client:
        await client.publish(
            topic="orders",
            data={"order_id": "ord-1"},
            attributes={"event_id": "e-1"},
        )
        await client.publish(
            topic="orders",
            data={"order_id": "ord-1"},
            attributes={"event_id": "e-1"},
        )

    assert processed == ["e-1"]


# --8<-- [end:idempotency_test_client]
