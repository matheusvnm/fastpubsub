from typing import Any

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


class GlobalLoggingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(f"[Global] Processing message: {message.id}")
        return await super().on_message(message)


# --8<-- [start:broker_include_middleware]
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
broker.include_middleware(GlobalLoggingMiddleware)
# --8<-- [end:broker_include_middleware]


# --8<-- [start:broker_constructor_middleware]
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(GlobalLoggingMiddleware)],
)
# --8<-- [end:broker_constructor_middleware]


app = FastPubSub(broker)

@broker.subscriber(
    "test-handler",
    topic_name="test-topic",
    subscription_name="test-subscription",
)
async def handle_message(message: Message) -> None:
    logger.info(f"Handler received: {message.data.decode()}")


@app.after_startup
async def publish_test():
    await broker.publish("test-topic", {"hello": "world"})
