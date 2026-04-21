from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="ecommerce-prod")
app = FastPubSub(broker)


# --8<-- [start:inspect_basic_subscribers]
@broker.subscriber(
    alias="process-orders",
    topic_name="order-events",
    subscription_name="orders-sub",
    ack_deadline_seconds=120,
    dead_letter_topic="order-events-dlq",
    max_delivery_attempts=5,
)
async def process_order(message: Message):
    """Validate and fulfill incoming orders."""
    ...


@broker.subscriber(
    alias="charge-payments",
    topic_name="payment-events",
    subscription_name="payments-sub",
    enable_exactly_once_delivery=True,
)
async def charge_payment(message: Message):
    """Process payment charges with exactly-once guarantees."""
    ...


@broker.subscriber(
    alias="send-notifications",
    topic_name="notification-events",
    subscription_name="notifications-sub",
    filter_expression='attributes.channel = "email"',
)
async def send_notification(message: Message):
    """Send email notifications filtered by channel attribute."""
    ...


# --8<-- [end:inspect_basic_subscribers]
