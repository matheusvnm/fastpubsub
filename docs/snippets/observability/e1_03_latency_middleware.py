import time
from datetime import UTC, datetime

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:latency_middleware]
class LatencyMiddleware(BaseMiddleware):
    async def on_message(self, message: Message):
        # Calculate message age from publisher-provided timestamp attribute.
        published_at = message.attributes.get("published_at")
        if published_at:
            published_at_dt = datetime.fromisoformat(published_at)
            age_seconds = (datetime.now(UTC) - published_at_dt).total_seconds()
            logger.info(f"Message age: {age_seconds:.2f}s")

            if age_seconds > 300:
                logger.warning(f"Old message detected: {age_seconds:.2f}s old")

        # Track processing time
        start_time = time.monotonic()
        result = await super().on_message(message)
        processing_time = (time.monotonic() - start_time) * 1000

        logger.info(f"Processing took {processing_time:.2f}ms")

        return result


# --8<-- [end:latency_middleware]


broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[Middleware(LatencyMiddleware)],
)
app = FastPubSub(broker)


# --8<-- [start:debug_handler]
@broker.subscriber(
    alias="debug-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handler(message: Message):
    logger.debug(
        "Full message",
        extra={
            "message_id": message.id,
            "data": message.data.decode("utf-8"),
            "attributes": message.attributes,
            "published_at": message.attributes.get("published_at"),
        },
    )


# --8<-- [end:debug_handler]
