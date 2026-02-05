from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
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


# --8<-- [start:publisher_in_startup]
@app.after_startup
async def test_publish() -> None:
    publisher: Publisher = broker.publisher("test-topic")
    await publisher.publish({"hello": "world"})


# --8<-- [end:publisher_in_startup]
