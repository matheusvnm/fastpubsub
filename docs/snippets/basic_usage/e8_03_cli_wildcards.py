from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

orders_router = PubSubRouter(prefix="orders")
payments_router = PubSubRouter(prefix="payments")


# --8<-- [start:wildcard_subscribers]
@orders_router.subscriber(
    alias="process",
    topic_name="new-orders",
    subscription_name="process-sub",
)
async def process_order(message: Message):
    """Process incoming orders."""
    pass


@orders_router.subscriber(
    alias="validate",
    topic_name="new-orders",
    subscription_name="validate-sub",
)
async def validate_order(message: Message):
    """Validate incoming orders."""
    pass


@orders_router.subscriber(
    alias="notify",
    topic_name="order-events",
    subscription_name="notify-sub",
)
async def notify_order(message: Message):
    """Send order notifications."""
    pass


@payments_router.subscriber(
    alias="process",
    topic_name="payment-events",
    subscription_name="process-sub",
)
async def process_payment(message: Message):
    """Process payments."""
    pass


broker.include_router(orders_router)
broker.include_router(payments_router)
# --8<-- [end:wildcard_subscribers]
