from fastpubsub.logger import logger
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.router import PubSubRouter


router = PubSubRouter(prefix="core")
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")

broker.include_router(router=router)
app = FastPubSub(broker)


@router.subscriber("test-alias",
                   topic_name="test-router-topic",
                   subscription_name="test-basic-router-subscription",)
async def handler_on_router(message: Message):
    logger.info(f"Processed message on router handler: {message}")


# The aliases/subscription name can be the same.
# That is because the PubSubRouter has prefix.
@broker.subscriber("test-alias",
                   topic_name="test-router-topic",
                   subscription_name="test-basic-router-subscription",)
async def handler_on_broker(message: Message):
    logger.info(f"Processed message on broker handler: {message}")


@app.after_startup
async def test_publish():
    await broker.publish("test-router-topic", {"hello": "world"})
