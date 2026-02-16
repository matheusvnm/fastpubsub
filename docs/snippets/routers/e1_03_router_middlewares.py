"""Title: Router with Middlewares

Demonstrates applying middlewares to all subscribers in a router.

This example shows:
- Creating a router with middlewares
- All subscribers in the router automatically get the middlewares applied
- Using Middleware wrapper to pass configuration

Run with:
    fastpubsub run docs.snippets.routers.e1_06_router_middlewares:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:router_middlewares_full]
from typing import Any

from fastpubsub import (
    BaseMiddleware,
    FastPubSub,
    Message,
    Middleware,
    PubSubBroker,
    PubSubRouter,
)
from fastpubsub.logger import logger


# --8<-- [start:domain_middleware]
class DomainLoggingMiddleware(BaseMiddleware):
    """Logs all messages in the domain."""

    async def on_message(self, message: Message) -> Any:
        logger.info(f"[Domain] Processing message: {message.id}")
        return await super().on_message(message)


# --8<-- [end:domain_middleware]


# --8<-- [start:router_with_middleware]
users_router = PubSubRouter(
    prefix="users",
    middlewares=[Middleware(DomainLoggingMiddleware)],
)
# --8<-- [end:router_with_middleware]


@users_router.subscriber(
    alias="created",
    topic_name="users-topic",
    subscription_name="users-subscription",
)
async def handle_user_created(message: Message):
    logger.info(f"User created: {message.data.decode()}")


@users_router.subscriber(
    alias="deleted",
    topic_name="users-deleted-topic",
    subscription_name="users-deleted-subscription",
)
async def handle_user_deleted(message: Message):
    logger.info(f"User deleted: {message.data.decode()}")


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_router(users_router)

app = FastPubSub(broker)


@app.after_startup
async def publish_test():
    await users_router.publish("users-topic", {"user": "test"})


# --8<-- [end:router_middlewares_full]
