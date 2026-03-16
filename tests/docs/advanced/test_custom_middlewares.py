import pytest

from docs.snippets.advanced.e1_01_custom_middlewares import (
    PublisherMetadataMiddleware,
    ValidationMiddleware,
)
from fastpubsub import Message, Middleware, PubSubBroker
from fastpubsub.exceptions import Drop
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.testing import PubSubTestClient


class _CapturePublishMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        super().__init__(next_call=None)
        self.calls: list[tuple[bytes, str, dict[str, str] | None]] = []

    async def on_publish(
        self,
        data: bytes,
        ordering_key: str,
        attributes: dict[str, str] | None,
    ) -> None:
        self.calls.append((data, ordering_key, attributes))


class TestAdvancedCustomMiddlewares:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_validation_middleware_drops_invalid_json(self) -> None:
        broker = PubSubBroker(
            project_id="test-project",
            middlewares=[Middleware(ValidationMiddleware)],
        )

        @broker.subscriber(alias="validator", topic_name="events", subscription_name="events-sub")
        async def _handler(message: Message) -> str:
            return message.data.decode("utf-8")

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="events", data=b'{"valid": true}')
            await client.publish(topic="events", data=b"{invalid-json")
            results = client.get_results()

        assert len(results) == 2
        assert results[0].error is None
        assert isinstance(results[1].error, Drop)

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_publisher_metadata_middleware_enriches_attributes(self) -> None:
        capture = _CapturePublishMiddleware()
        middleware = PublisherMetadataMiddleware(next_call=capture)

        await middleware.on_publish(
            data=b"payload", ordering_key="order-1", attributes={"tenant": "acme"}
        )

        assert len(capture.calls) == 1
        _, ordering_key, attributes = capture.calls[0]
        assert ordering_key == "order-1"
        assert attributes == {
            "tenant": "acme",
            "schema-version": "v1",
            "source-service": "orders-service",
        }

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_publisher_metadata_middleware_handles_missing_attributes(self) -> None:
        capture = _CapturePublishMiddleware()
        middleware = PublisherMetadataMiddleware(next_call=capture)

        await middleware.on_publish(data=b"payload", ordering_key="", attributes=None)

        assert capture.calls[0][2] == {
            "schema-version": "v1",
            "source-service": "orders-service",
        }
