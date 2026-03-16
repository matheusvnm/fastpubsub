import time
from typing import Any

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:logging_middleware]
class FullLoggingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        start_time = time.monotonic()

        try:
            # Call the next middleware or handler
            response = await super().on_message(message)

            processing_time = (time.monotonic() - start_time) * 1000
            logger.info(f"Message processed in {processing_time:.2f}ms")

            return response
        except Exception as e:
            logger.error(
                f"Message {message.id} failed with error: {e}",
                extra={"message_id": message.id},
            )
            # Re-raise to trigger nack
            raise

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(f"Publishing message with {len(data)} bytes")

        if attributes is None:
            attributes = {}

        # Add a trace ID to all outgoing messages
        attributes["x-trace-id"] = "some-trace-id"

        # Call the next middleware or publisher
        return await super().on_publish(data, ordering_key, attributes)


# --8<-- [end:logging_middleware]


broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(FullLoggingMiddleware)],
)
app = FastPubSub(broker)


@broker.subscriber(
    "test-handler",
    topic_name="test-topic",
    subscription_name="test-subscription",
)
async def handle_message(message: Message) -> None:
    logger.info(f"Handler received: {message.data.decode()}")


@app.after_startup
async def publish_first_message():
    await broker.publish("test-topic", {"hello": "world"})
