from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


# --8<-- [start:multiple_subscribers]
@broker.subscriber(
    alias="process-orders",
    topic_name="orders",
    subscription_name="orders-sub",
)
async def handle_orders(message: Message):
    """Process order messages."""
    pass


@broker.subscriber(
    alias="send-notifications",
    topic_name="notifications",
    subscription_name="notifications-sub",
)
async def handle_notifications(message: Message):
    """Process notification messages."""
    pass
# --8<-- [end:multiple_subscribers]
