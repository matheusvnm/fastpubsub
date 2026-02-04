from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

# --8<-- [start:nested_setup]
# Level 2: Sub-domain routers
banking_router = PubSubRouter(prefix="banking")
finance_router = PubSubRouter(prefix="finance")

# Level 1: Main domain router
core_router = PubSubRouter(prefix="core")
core_router.include_router(banking_router)
core_router.include_router(finance_router)
# --8<-- [end:nested_setup]


# --8<-- [start:nested_handlers]
@core_router.subscriber(
    alias="core_handler",
    topic_name="core-topic",
    subscription_name="core-subscription",
)
async def handle_message_core(message: Message):
    logger.info(f"CORE handler received message {message.id}")


@banking_router.subscriber(
    alias="banking_handler",
    topic_name="banking-topic",
    subscription_name="banking-subscription",
)
async def handle_message_banking(message: Message):
    logger.info(f"BANKING handler received message {message.id}")


@finance_router.subscriber(
    alias="finance_handler",
    topic_name="finance-topic",
    subscription_name="finance-subscription",
)
async def handle_message_finance(message: Message):
    logger.info(f"FINANCE handler received message {message.id}")
# --8<-- [end:nested_handlers]

# --8<-- [start:nested_broker]
# Application setup
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(core_router)

app = FastPubSub(broker)
# --8<-- [end:nested_broker]

@app.after_startup
async def publish_test_messages():
    await broker.publish("core-topic", {"event": "core"})
    await broker.publish("banking-topic", {"event": "banking"})
    await broker.publish("finance-topic", {"event": "finance"})
