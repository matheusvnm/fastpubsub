from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.datastructures import PullMethod
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "streaming-pull-alias",
    topic_name="streaming-pull-topic",
    subscription_name="streaming-pull-subscription",
    pull_method=PullMethod.STREAMING_PULL,
)
async def process_message_streaming_pull(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@broker.subscriber(
    "unary-pull-alias",
    topic_name="unary-pull-topic",
    subscription_name="unary-pull-subscription",
    pull_method=PullMethod.UNARY_PULL,
)
async def process_message_unary_pull(message: Message) -> None:
    logger.info(f"Processed message: {message}")


@app.after_startup
async def test_publish() -> None:
    for _ in range(5):
        await broker.publish("streaming-pull-topic", {"message": "streaming"})
        await broker.publish("unary-pull-topic", {"message": "unary"})
