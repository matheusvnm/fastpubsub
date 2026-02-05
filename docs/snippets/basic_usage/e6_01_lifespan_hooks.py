from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:on_startup]
@app.on_startup
async def setup_database():
    logger.info("Connecting to the database...")
    # Example: app.state.db_pool = await asyncpg.create_pool(...)
    logger.info("Database connection pool created.")


# --8<-- [end:on_startup]


# --8<-- [start:after_startup]
@app.after_startup
async def announce_startup():
    logger.info("Subscribers are running. Publishing startup message.")
    await broker.publish("system-logs", data={"status": "online"})


# --8<-- [end:after_startup]


# --8<-- [start:on_shutdown]
@app.on_shutdown
async def prepare_for_shutdown():
    logger.info("Shutdown signal received. Preparing to stop...")
    app.state.is_shutting_down = True


# --8<-- [end:on_shutdown]


# --8<-- [start:after_shutdown]
@app.after_shutdown
async def cleanup_database():
    logger.info("Closing database connection pool...")
    # Example: await app.state.db_pool.close()
    logger.info("Database pool closed.")


# --8<-- [end:after_shutdown]


@broker.subscriber(
    "system-handler",
    topic_name="system-logs",
    subscription_name="system-logs-subscription",
)
async def handle_system_log(message: Message) -> None:
    logger.info(f"System log: {message.data.decode()}")
