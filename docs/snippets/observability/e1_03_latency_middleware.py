"""Title: Latency Tracking Middleware

Demonstrates performance monitoring with latency tracking.

This example shows:
- Calculating message age
- Tracking processing time
- Logging performance metrics

Run with:
    fastpubsub run docs.snippets.observability.e1_03_latency_middleware:app

Requirements:
    - Set PUBSUB_EMULATOR_HOST for local testing, or
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
"""

# --8<-- [start:latency_full]
import time
from datetime import UTC, datetime

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:latency_middleware]
class LatencyMiddleware(BaseMiddleware):
    async def on_message(self, message: Message):
        # Calculate message age
        if message.publish_time:
            age_seconds = (datetime.now(UTC) - message.publish_time).total_seconds()
            logger.info(f"Message age: {age_seconds:.2f}s")

            if age_seconds > 300:  # 5 minutes
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
            "ordering_key": message.ordering_key,
            "publish_time": message.publish_time.isoformat() if message.publish_time else None,
        },
    )


# --8<-- [end:debug_handler]
# --8<-- [end:latency_full]
