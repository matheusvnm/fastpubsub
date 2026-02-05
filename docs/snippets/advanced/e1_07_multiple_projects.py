from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter

broker = PubSubBroker(project_id="project-a")
app = FastPubSub(broker)


async def process_local_event(data: bytes) -> None:
    """Process local event."""
    pass


async def process_shared_event(data: bytes) -> None:
    """Process shared event."""
    pass


async def process_analytics(data: bytes) -> None:
    """Process analytics."""
    pass


async def process_order(data: bytes) -> None:
    """Process order."""
    pass


async def sync_user_data(data: bytes) -> None:
    """Sync user data."""
    pass


async def send_notification(data: bytes) -> None:
    """Send notification."""
    pass


# --8<-- [start:cross_project_subscriber]
# This subscriber uses the default project (project-a)
@broker.subscriber(
    alias="local-handler",
    topic_name="local-events",
    subscription_name="local-events-subscription",
)
async def handle_local_events(message: Message):
    await process_local_event(message.data)


# This subscriber uses a different project (project-b)
@broker.subscriber(
    alias="cross-project-handler",
    topic_name="shared-events",
    subscription_name="project-a-subscription",
    project_id="project-b",
    autocreate=True,
)
async def handle_cross_project_events(message: Message):
    await process_shared_event(message.data)


# --8<-- [end:cross_project_subscriber]


# --8<-- [start:cross_project_publisher]
# Publisher for the default project
local_publisher = broker.publisher("local-events")

# Publisher for a different project
cross_project_publisher = broker.publisher("shared-events", project_id="project-b")


@app.post("/send-event")
async def send_event(data: dict):
    # Publish to local project
    await local_publisher.publish(data)

    # Publish to other project
    await cross_project_publisher.publish(data)


# --8<-- [end:cross_project_publisher]


# --8<-- [start:router_cross_project]
# Router for external project
external_router = PubSubRouter(prefix="external", project_id="project-b")


@external_router.subscriber(
    alias="shared-handler",
    topic_name="shared-events",
    subscription_name="project-a-subscription",
)
async def handle_shared(message: Message):
    await process_shared_event(message.data)


@external_router.subscriber(
    alias="analytics-handler",
    topic_name="analytics-events",
    subscription_name="project-a-analytics-subscription",
)
async def handle_analytics(message: Message):
    await process_analytics(message.data)


# Include the router in the broker
broker.include_router(external_router)
# --8<-- [end:router_cross_project]


# --8<-- [start:nested_routers]
# Create a separate broker for nested router demo
nested_broker = PubSubBroker(project_id="project-a")

# First level router - uses project-b
level1_router = PubSubRouter(prefix="external", project_id="project-b")

# Second level router - uses project-c
level2_router = PubSubRouter(prefix="analytics", project_id="project-c")


# Subscriber uses project-c (inherited from level2)
@level2_router.subscriber(
    alias="handler",
    topic_name="metrics",
    subscription_name="metrics-subscription",
)
async def handle_metrics(message: Message):
    pass


level1_router.include_router(level2_router)
nested_broker.include_router(level1_router)
# --8<-- [end:nested_routers]


# --8<-- [start:complete_example]
# Complete example: service consuming from multiple projects
complete_broker = PubSubBroker(project_id="my-service")
complete_app = FastPubSub(complete_broker)


# Local events
@complete_broker.subscriber(
    alias="local-orders",
    topic_name="orders",
    subscription_name="orders-subscription",
)
async def handle_local_orders(message: Message):
    await process_order(message.data)


# Events from shared platform
platform_router = PubSubRouter(prefix="platform", project_id="shared-platform")


@platform_router.subscriber(
    alias="user-events",
    topic_name="user-events",
    subscription_name="my-service-user-subscription",
)
async def handle_user_events(message: Message):
    await sync_user_data(message.data)


@platform_router.subscriber(
    alias="notifications",
    topic_name="notifications",
    subscription_name="my-service-notifications-subscription",
)
async def handle_notifications(message: Message):
    await send_notification(message.data)


complete_broker.include_router(platform_router)


# Publishing to both projects
@complete_app.post("/create-order")
async def create_order(order: dict):
    # Local publish
    await complete_broker.publish("orders", order)

    # Notify platform
    await complete_broker.publish(
        "order-events",
        {"order_id": order["id"], "action": "created"},
        project_id="shared-platform",
    )


# --8<-- [end:complete_example]
