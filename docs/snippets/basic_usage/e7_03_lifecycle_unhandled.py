import json

from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def process(data: dict) -> None:
    """Process the event data."""
    pass


# --8<-- [start:unhandled_handler]
@broker.subscriber(
    alias="event-processor",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_event(message: Message):
    # If this raises ValueError, KeyError, etc.
    # the message is nacked and redelivered
    data = json.loads(message.data)
    await process(data)
# --8<-- [end:unhandled_handler]
