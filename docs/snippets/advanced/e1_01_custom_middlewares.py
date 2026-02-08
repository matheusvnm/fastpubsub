import asyncio
import gzip
import json
import time
from typing import Any

from fastpubsub import BaseMiddleware, FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import logger


# --8<-- [start:rate_limit_middleware]
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, next_call: BaseMiddleware, requests_per_second: int = 100):
        super().__init__(next_call)

        self.requests_per_second = requests_per_second
        self.tokens = requests_per_second
        self.last_update = time.monotonic()

    async def on_message(self, message: Message) -> Any:
        await self._acquire_token()
        return await super().on_message(message)

    async def _acquire_token(self):
        # Token bucket implementation
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(
            self.requests_per_second, self.tokens + elapsed * self.requests_per_second
        )
        self.last_update = now

        if self.tokens < 1:
            await asyncio.sleep(1 / self.requests_per_second)
            self.tokens = 1

        self.tokens -= 1


# --8<-- [end:rate_limit_middleware]


# --8<-- [start:validation_middleware]
class ValidationMiddleware(BaseMiddleware):
    """Only validates incoming messages."""

    async def on_message(self, message: Message) -> Any:
        # Validate message data
        if not self._is_valid(message.data):
            raise Drop("Invalid message format")
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        # Pass through without modification
        return await super().on_publish(data, ordering_key, attributes)

    def _is_valid(self, data: bytes) -> bool:
        try:
            json.loads(data)
            return True
        except json.JSONDecodeError:
            return False


# --8<-- [end:validation_middleware]


# --8<-- [start:compression_middleware]
class CompressionMiddleware(BaseMiddleware):
    """Only compresses outgoing messages."""

    async def on_message(self, message: Message) -> Any:
        # Pass through without modification
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        # Compress data before sending
        compressed = gzip.compress(data)
        if attributes is None:
            attributes = {}
        attributes["content-encoding"] = "gzip"
        return await super().on_publish(compressed, ordering_key, attributes)


# --8<-- [end:compression_middleware]


# Custom exception types for demonstration
class ValidationError(Exception):
    pass


class TemporaryError(Exception):
    pass


# --8<-- [start:error_handling_middleware]
class ErrorHandlingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        try:
            return await super().on_message(message)

        except ValidationError as e:
            # Invalid data - don't retry, just drop
            logger.warning(f"Dropping invalid message: {e}")
            raise Drop(f"Validation failed: {e}")

        except TemporaryError as e:
            # Temporary issue - retry later
            logger.info(f"Retrying message due to: {e}")
            raise Retry(f"Temporary failure: {e}")

        except Exception:
            # Unexpected error - log and let it propagate
            logger.exception(f"Unexpected error processing message {message.id}")
            raise


# --8<-- [end:error_handling_middleware]


# --8<-- [start:metrics_middleware]
# Simulated Prometheus metrics (use prometheus_client in production)
class MetricsMiddleware(BaseMiddleware):
    def __init__(self, next_call: BaseMiddleware, subscriber_name: str):
        super().__init__(next_call)
        self.subscriber_name = subscriber_name

    async def on_message(self, message: Message) -> Any:
        start = time.monotonic()
        status = "success"

        try:
            result = await super().on_message(message)
            return result
        except Exception:
            status = "error"
            raise
        finally:
            duration = time.monotonic() - start
            # In production, use prometheus_client:
            # MESSAGES_PROCESSED.labels(subscriber=self.subscriber_name, status=status).inc()
            # PROCESSING_TIME.labels(subscriber=self.subscriber_name).observe(duration)
            logger.info(
                f"Metrics: subscriber={self.subscriber_name} status={status} duration={duration:.3f}s"
            )


# --8<-- [end:metrics_middleware]


# --8<-- [start:middleware_composition]
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[
        Middleware(ErrorHandlingMiddleware),
        Middleware(MetricsMiddleware, subscriber_name="orders"),
        Middleware(ValidationMiddleware),
    ],
)
# --8<-- [end:middleware_composition]

app = FastPubSub(broker)


@broker.subscriber(
    alias="middleware-demo",
    topic_name="middleware-demo",
    subscription_name="middleware-demo-subscription",
)
async def handle_message(message: Message):
    logger.info(f"Processing message: {message.id}")
