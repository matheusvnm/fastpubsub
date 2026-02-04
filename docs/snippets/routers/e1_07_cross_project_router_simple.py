from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")


# --8<-- [start:cross_project_router]
# All subscribers in this router use project-b
external_router = PubSubRouter(
    prefix="external",
    project_id="project-b",
)

@external_router.subscriber(
    alias="handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_external_event(message: Message):
    logger.info(f"External event: {message.data.decode()}")


broker.include_router(external_router)
# --8<-- [end:cross_project_router]


app = FastPubSub(broker)

@app.after_startup
async def publish_test():
    await external_router.publish("events", {"event": "test"})
