from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-publish",
)
async def handle(message: Message) -> None:
    logger.info(f"Processed message: {message}")


# --8<-- [start:broker_publish]
@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", {"hello": "world"})

# --8<-- [end:broker_publish]

