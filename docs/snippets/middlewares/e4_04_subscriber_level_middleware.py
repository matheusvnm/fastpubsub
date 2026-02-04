from typing import Any

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


class DebugMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(f"[Debug] Raw message data: {message.data}")
        return await super().on_message(message)


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:subscriber_middleware]
@broker.subscriber(
    alias="debug-handler",
    topic_name="events",
    subscription_name="events-subscription",
    middlewares=[Middleware(DebugMiddleware)],
)
async def handle_message(message: Message):
    print(message)

# --8<-- [end:subscriber_middleware]


@app.after_startup
async def publish_test():
    await broker.publish("events", {"event": "test"})
# --8<-- [end:subscriber_level_full]
