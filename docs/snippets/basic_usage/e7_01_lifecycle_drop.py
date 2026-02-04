from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Drop

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:drop_handler]
@broker.subscriber(
    alias="event-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_events(message: Message):
    event_attributes = message.attributes
    if event_attributes.get("schema_version") == "v1":
        # We no longer support v1 events
        raise Drop("Schema version v1 is deprecated.")

    # Process v2+ events...
# --8<-- [end:drop_handler]
