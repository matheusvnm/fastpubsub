from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:basic_logger]
@broker.subscriber(
    alias="task-handler",
    topic_name="tasks",
    subscription_name="tasks-subscription",
)
async def handle_task(message: Message):
    logger.info("This is a log message!")


# --8<-- [end:basic_logger]


# --8<-- [start:context_aware_logging]
@broker.subscriber(
    alias="order-handler",
    topic_name="orders",
    subscription_name="orders-subscription",
)
async def handle_order(message: Message):
    logger.info("Processing order")
    # Output:
    # Processing order | name=order-handler message_id=12345 topic_name=orders


# --8<-- [end:context_aware_logging]


# --8<-- [start:custom_context]
@broker.subscriber(
    alias="user-task-handler",
    topic_name="user-tasks",
    subscription_name="user-tasks-subscription",
)
async def handle_user_task(message: Message):
    user_id = message.attributes.get("user_id")

    with logger.contextualize(user_id=user_id):
        logger.info("Processing task for user.")
        # ... some work ...
        logger.warning("User processing had a minor issue.")

    logger.info("This log will NOT have the user_id tag.")


# --8<-- [end:custom_context]


# --8<-- [start:log_levels]
@broker.subscriber(
    alias="levels-handler",
    topic_name="levels",
    subscription_name="levels-subscription",
)
async def handler(message: Message):
    order_id = "12345"
    sku = "SKU-001"
    qty = 5
    reason = "insufficient funds"
    attempts = 3

    logger.debug("Detailed debug info", extra={"raw_data": message.data})
    logger.info("Processing order", extra={"order_id": order_id})
    logger.warning("Inventory low", extra={"sku": sku, "quantity": qty})
    logger.error("Payment failed", extra={"reason": reason})
    logger.critical("Database unreachable", extra={"attempts": attempts})


# --8<-- [end:log_levels]
