"""Title: Logging Middleware Example

Demonstrates a complete logging middleware with timing and error handling.
"""

import time
from typing import Any

from fastpubsub import BaseMiddleware, Message
from fastpubsub.logger import logger


# --8<-- [start:full_logging_middleware]
class FullLoggingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        start_time = time.monotonic()

        try:
            response = await super().on_message(message)
            processing_time = (time.monotonic() - start_time) * 1000
            logger.info(f"Message processed in {processing_time:.2f}ms")
            return response
        except Exception as e:
            logger.error(
                f"Message {message.id} failed with error: {e}",
                extra={"message_id": message.id},
            )
            raise

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(f"Publishing message with {len(data)} bytes")

        if attributes is None:
            attributes = {}

        attributes["x-trace-id"] = "some-trace-id"
        await super().on_publish(data, ordering_key, attributes)
# --8<-- [end:full_logging_middleware]
