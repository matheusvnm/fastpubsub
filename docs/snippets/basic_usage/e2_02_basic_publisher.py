from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)

# --8<-- [start:publisher_instance]
publisher: Publisher = broker.publisher("test-topic")
# --8<-- [end:publisher_instance]


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-publish",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")


# --8<-- [start:publisher_instance_publish]
@app.after_startup
async def test_publish() -> None:
    await publisher.publish({"hello": "world"})

# --8<-- [end:publisher_instance_publish]
