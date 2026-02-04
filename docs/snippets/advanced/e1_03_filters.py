from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def process_order(order_data: bytes) -> None:
    """Process order data."""
    pass


async def escalate_to_senior_support(data: bytes) -> None:
    """Escalate to senior support."""
    pass


async def page_on_call_engineer(data: bytes) -> None:
    """Page on-call engineer."""
    pass


async def process_user_event(data: bytes) -> None:
    """Process user event."""
    pass


async def log_to_audit_trail(data: bytes) -> None:
    """Log to audit trail."""
    pass


# --8<-- [start:basic_filter]
@broker.subscriber(
    alias="order-handler",
    topic_name="events",
    subscription_name="order-events-subscription",
    filter_expression='attributes.event_type = "order"',
    autocreate=True,
)
async def handle_orders(message: Message):
    # Only receives messages where event_type = "order"
    order_data = message.data
    await process_order(order_data)
# --8<-- [end:basic_filter]


# --8<-- [start:filter_and]
@broker.subscriber(
    alias="premium-urgent",
    topic_name="tickets",
    subscription_name="premium-urgent-subscription",
    filter_expression='attributes.priority = "high" AND attributes.customer_tier = "premium"',
)
async def handle_premium_urgent(message: Message):
    # Only receives high-priority tickets from premium customers
    await escalate_to_senior_support(message.data)
# --8<-- [end:filter_and]


# --8<-- [start:filter_or]
@broker.subscriber(
    alias="critical-alerts",
    topic_name="alerts",
    subscription_name="critical-alerts-subscription",
    filter_expression='attributes.severity = "critical" OR attributes.severity = "high"',
)
async def handle_critical_alerts(message: Message):
    # Receives both critical and high severity alerts
    await page_on_call_engineer(message.data)
# --8<-- [end:filter_or]


# --8<-- [start:filter_has_prefix]
@broker.subscriber(
    alias="labeled-handler",
    topic_name="labeled-events",
    subscription_name="labeled-subscription",
    filter_expression='hasPrefix(attributes.label, "")',
)
async def handle_labeled(message: Message):
    # Receives any message that has a "label" attribute
    pass
# --8<-- [end:filter_has_prefix]


# --8<-- [start:multiple_subscribers]
# Handler for order events
@broker.subscriber(
    alias="order-events-handler",
    topic_name="multi-events",
    subscription_name="order-events-sub",
    filter_expression='attributes.event_type = "order"',
)
async def handle_order_events(message: Message):
    await process_order(message.data)


# Handler for user events
@broker.subscriber(
    alias="user-handler",
    topic_name="multi-events",
    subscription_name="user-events-sub",
    filter_expression='attributes.event_type = "user"',
)
async def handle_users(message: Message):
    await process_user_event(message.data)


# Handler for ALL events (no filter)
@broker.subscriber(
    alias="audit-handler",
    topic_name="multi-events",
    subscription_name="audit-sub",
)
async def audit_all_events(message: Message):
    await log_to_audit_trail(message.data)
# --8<-- [end:multiple_subscribers]
