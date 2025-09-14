from fastpubsub.logger import logger
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.routing.router import PubSubRouter


# The aliases/subscription name can be the same.
# That is because each PubSubRouter has prefix.
# Hence, their message handler do not conflict
router_core = PubSubRouter(prefix="core")
router_sales = PubSubRouter(prefix="sales")
router_logistics = PubSubRouter(prefix="logistics")

router_core.include_router(router_sales)
router_core.include_router(router_logistics)


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(router_core)


app = FastPubSub(broker)


@router_core.subscriber("some-alias",
                   topic_name="some-router-topic",
                   subscription_name="some-router-sub",)
async def handler_on_core_router(message: Message):
    logger.info(f"Processed message on core router: {message}")


@router_sales.subscriber("some-alias",
                   topic_name="some-router-topic",
                   subscription_name="some-router-sub",)
async def handler_on_sales_router(message: Message):
    logger.info(f"Processed message on sales router: {message}")



@router_logistics.subscriber("some-alias",
                   topic_name="some-router-topic",
                   subscription_name="some-router-sub",)
async def handler_on_logistics_router(message: Message):
    logger.info(f"Processed message on logistics handler: {message}")


@app.after_startup
async def test_publish():
    await broker.publish("some-router-topic", {"hello": "world"})
