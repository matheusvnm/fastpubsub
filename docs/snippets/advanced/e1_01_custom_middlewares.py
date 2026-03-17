import json
import time
from typing import Any

import pytest

from fastpubsub import BaseMiddleware, Message, Middleware, PubSubBroker
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import logger
from fastpubsub.testing import PubSubTestClient


# --8<-- [start:rate_limit_middleware]
class RateLimiterService:
    """External rate limiter contract (Redis, API gateway, etc.)."""

    async def acquire(self, key: str) -> None:
        pass


class RateLimitMiddleware(BaseMiddleware):
    """Rate limiting middleware that delegates state to an external service."""

    def __init__(self, next_call: BaseMiddleware, limiter: RateLimiterService):
        super().__init__(next_call)
        self.limiter = limiter

    async def on_message(self, message: Message) -> Any:
        await self.limiter.acquire(key=message.subscriber_name)
        return await super().on_message(message)


# --8<-- [end:rate_limit_middleware]


# --8<-- [start:configured_middleware_registration]
rate_limiter = RateLimiterService()

broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[
        Middleware(RateLimitMiddleware, limiter=rate_limiter),
    ],
)
# --8<-- [end:configured_middleware_registration]


# --8<-- [start:validation_middleware]
class ValidationMiddleware(BaseMiddleware):
    """Rejects non-JSON payloads before they reach the handler."""

    async def on_message(self, message: Message) -> Any:
        if not self._is_valid_json(message.data):
            raise Drop("Invalid payload: expected JSON bytes")

        return await super().on_message(message)

    @staticmethod
    def _is_valid_json(data: bytes) -> bool:
        try:
            json.loads(data)
            return True
        except json.JSONDecodeError:
            return False


# --8<-- [end:validation_middleware]


# --8<-- [start:publisher_metadata_middleware]
class PublisherMetadataMiddleware(BaseMiddleware):
    """Adds delivery metadata to outgoing messages."""

    async def on_publish(
        self,
        data: bytes,
        ordering_key: str,
        attributes: dict[str, str] | None,
    ) -> Any:
        metadata = {} if attributes is None else dict(attributes)
        metadata["schema-version"] = "v1"
        metadata["source-service"] = "orders-service"
        return await super().on_publish(data, ordering_key, metadata)


# --8<-- [end:publisher_metadata_middleware]


# --8<-- [start:error_handling_middleware]
class ErrorHandlingMiddleware(BaseMiddleware):
    """Maps domain errors to explicit lifecycle outcomes."""

    async def on_message(self, message: Message) -> Any:
        try:
            return await super().on_message(message)
        except ValueError as error:
            logger.warning(
                "Dropping message due to validation error",
                extra={"error": str(error)},
            )
            raise Drop(str(error)) from error
        except TimeoutError as error:
            logger.info(
                "Retrying message due to transient timeout",
                extra={"error": str(error)},
            )
            raise Retry(str(error)) from error


# --8<-- [end:error_handling_middleware]


# --8<-- [start:metrics_middleware]
class MetricsMiddleware(BaseMiddleware):
    """Records processing latency and status per subscriber."""

    def __init__(self, next_call: BaseMiddleware, subscriber_name: str):
        super().__init__(next_call)
        self.subscriber_name = subscriber_name

    async def on_message(self, message: Message) -> Any:
        start = time.monotonic()
        status = "success"

        try:
            return await super().on_message(message)
        except Exception:
            status = "error"
            raise
        finally:
            elapsed = time.monotonic() - start
            logger.info(
                "subscriber.metrics",
                extra={
                    "subscriber": self.subscriber_name,
                    "status": status,
                    "latency_seconds": f"{elapsed:.6f}",
                },
            )


# --8<-- [end:metrics_middleware]


# --8<-- [start:middleware_composition]
broker_with_composition = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    middlewares=[
        Middleware(ErrorHandlingMiddleware),
        Middleware(MetricsMiddleware, subscriber_name="orders-handler"),
        Middleware(ValidationMiddleware),
    ],
)
# --8<-- [end:middleware_composition]


# --8<-- [start:middleware_integration_test]
@pytest.mark.asyncio
async def test_validation_middleware_drops_invalid_payload() -> None:
    test_broker = PubSubBroker(
        project_id="test-project",
        middlewares=[Middleware(ValidationMiddleware)],
    )

    @test_broker.subscriber(
        alias="validator",
        topic_name="events",
        subscription_name="events-subscription",
    )
    async def handler(message: Message) -> str:
        return message.data.decode("utf-8")

    async with PubSubTestClient(test_broker) as client:
        await client.publish(topic="events", data=b'{"valid":true}')
        await client.publish(topic="events", data=b"{invalid-json")

        results = client.get_results()

    assert len(results) == 2
    assert results[0].error is None
    assert isinstance(results[1].error, Drop)


# --8<-- [end:middleware_integration_test]
