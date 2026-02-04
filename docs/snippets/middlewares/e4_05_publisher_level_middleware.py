from typing import Any

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


class AddTraceMiddleware(BaseMiddleware):
    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        if attributes is None:
            attributes = {}
        attributes["x-trace-id"] = "generated-trace-id"
        logger.info("[Trace] Adding trace ID to message")
        return await super().on_publish(data, ordering_key, attributes)


broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:publisher_middleware]
my_publisher = broker.publisher("events")
my_publisher.include_middleware(AddTraceMiddleware)
# --8<-- [end:publisher_middleware]


@broker.subscriber(
    "events-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_event(message: Message):
    logger.info(f"Received: {message.data.decode()}")
    logger.info(f"Trace ID: {message.attributes.get('x-trace-id')}")


# --8<-- [start:publisher_usage]
@app.after_startup
async def publish_with_trace():
    await my_publisher.publish(data={"hello": "world"})
# --8<-- [end:publisher_usage]
