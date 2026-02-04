from contextlib import asynccontextmanager

import httpx

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:custom_lifespan]
@asynccontextmanager
async def global_lifespan(app: FastPubSub):
    logger.info("GLOBAL LIFESPAN: Starting up...")
    # Create a shared HTTP client
    async with httpx.AsyncClient() as client:
        app.state.http_client = client
        logger.info("GLOBAL LIFESPAN: HTTP client created.")
        yield
    logger.info("GLOBAL LIFESPAN: HTTP client closed.")
    logger.info("GLOBAL LIFESPAN: Shutting down...")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker, lifespan=global_lifespan)
# --8<-- [end:custom_lifespan]


@app.on_startup
async def on_startup_hook():
    # Runs after http_client is created
    logger.info("  FastPubSub Hook: @app.on_startup")


@app.after_startup
async def after_startup_hook():
    logger.info("  FastPubSub Hook: @app.after_startup (Broker is running)")


@app.on_shutdown
async def on_shutdown_hook():
    logger.info("  FastPubSub Hook: @app.on_shutdown")


@app.after_shutdown
async def after_shutdown_hook():
    logger.info("  FastPubSub Hook: @app.after_shutdown (Broker is stopped)")


@broker.subscriber(
    "test-handler",
    topic_name="test-topic",
    subscription_name="test-subscription",
)
async def handle_message(message: Message) -> None:
    logger.info(f"Received: {message.data.decode()}")