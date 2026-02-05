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

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")


class UserAuthMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(f"[Users] Auth check for message: {message.id}")
        return await super().on_message(message)


# --8<-- [start:router_include_middleware]
users_router = PubSubRouter(prefix="users")
users_router.include_middleware(UserAuthMiddleware)
# --8<-- [end:router_include_middleware]


# --8<-- [start:router_constructor_middleware]
banking_router = PubSubRouter(
    prefix="banking",
    middlewares=[Middleware(UserAuthMiddleware)],
)
# --8<-- [end:router_constructor_middleware]

broker.include_router(users_router)
broker.include_router(banking_router)
app = FastPubSub(broker)


@users_router.subscriber(
    "created",
    topic_name="users-topic",
    subscription_name="users-subscription",
)
async def handle_user_created(message: Message) -> None:
    logger.info(f"User created: {message.data.decode()}")


@app.after_startup
async def publish_test():
    await users_router.publish("users-topic", {"user": "test"})
