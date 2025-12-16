from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.router import PubSubRouter

first_unnamed_router = PubSubRouter(prefix="")
second_unnamed_router = PubSubRouter(prefix="")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router=first_unnamed_router)
broker.include_router(router=second_unnamed_router)

app = FastPubSub(broker)


@first_unnamed_router.subscriber(
    "test-alias-abc",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_first_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on first unnamed router: {message}")


@second_unnamed_router.subscriber(
    "test-alias-cba",
    topic_name="test-router-topic",
    subscription_name="test-basic-router-subscription",
)
async def handle_on_second_unnamed_router(message: Message) -> None:
    logger.info(f"Processed message on second unnamed router: {message}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-router-topic", {"hello": "world"})
