from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "subscriber-alias",
    topic_name="subscriber-topic",
    subscription_name="subscriber-subscription",
)
async def process_message(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("subscriber-topic", {"message": "streaming a message"})
