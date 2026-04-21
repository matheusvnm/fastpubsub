from fastpubsub import (
    FastPubSub,
    Message,
    PubSubBroker,
    PubSubRouter,
)

broker = PubSubBroker(project_id="saas-platform")
app = FastPubSub(broker)


# --8<-- [start:inspect_router_subscribers]
orders_router = PubSubRouter(prefix="orders")
analytics_router = PubSubRouter(prefix="analytics")
platform_router = PubSubRouter(
    prefix="platform",
    routers=[orders_router, analytics_router],
)


@orders_router.subscriber(
    alias="fulfill",
    topic_name="order-events",
    subscription_name="fulfill-sub",
)
async def fulfill_order(message: Message):
    """Pick, pack, and ship the order."""
    ...


@orders_router.subscriber(
    alias="invoice",
    topic_name="order-events",
    subscription_name="invoice-sub",
)
async def create_invoice(message: Message):
    """Generate and store the invoice."""
    ...


@analytics_router.subscriber(
    alias="track",
    topic_name="analytics-events",
    subscription_name="track-sub",
    project_id="analytics-warehouse",
)
async def track_event(message: Message):
    """Forward events to the analytics data warehouse."""
    ...


broker.include_router(platform_router)
# --8<-- [end:inspect_router_subscribers]
