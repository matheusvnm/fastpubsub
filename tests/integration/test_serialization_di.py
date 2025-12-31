"""Integration tests for serialization and dependency injection."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Annotated

import pytest
from pydantic import BaseModel

from fastpubsub import Body, Header, Message, PullMessage
from fastpubsub.serialization import DefaultSerializer
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker


class UserEvent(BaseModel):
    """Sample event model for testing."""

    user_id: str
    action: str
    data: dict


@pytest.mark.connected
class TestBackwardsCompatibility:
    """Tests for backwards compatibility with existing handlers."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_raw_message_handler_still_works(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Existing handlers that take raw Message should still work."""
        received: asyncio.Queue[Message] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-raw-msg",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(message: Message) -> None:
            await received.put(message)

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data={"user_id": "123", "action": "click"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert isinstance(result, PullMessage)
            decoded = json.loads(result.data)
            assert decoded["user_id"] == "123"

    @pytest.mark.asyncio
    async def test_single_untyped_param_handler(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Single param with no type hint should receive raw Message."""
        received: asyncio.Queue[Message] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-untyped",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg) -> None:
            await received.put(msg.decode())

        async with managed_broker(connected_broker):
            expected_result = "test message"
            await connected_broker.publish(
                topic_name=unique_topic,
                data=expected_result,
            )
            received_result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert received_result == expected_result


@pytest.mark.connected
class TestAutoUnwrap:
    """Tests for automatic parameter unwrapping from decoded message."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_single_unannotated_param_gets_entire_body(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Single unannotated param should receive entire decoded body."""
        received: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-single-unwrap",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(data: dict) -> None:
            await received.put(data)

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data={"user_id": "123", "action": "click"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert result == {"user_id": "123", "action": "click"}

    @pytest.mark.asyncio
    async def test_multiple_unannotated_params_extract_from_dict(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Multiple unannotated params should extract matching keys from dict."""
        received: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-multi-unwrap",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(user_id: str, action: str) -> None:
            await received.put((user_id, action))

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data={"user_id": "user123", "action": "purchase", "extra": "ignored"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert result == ("user123", "purchase")


@pytest.mark.connected
class TestHeaderAnnotation:
    """Tests for Header annotation extracting from message attributes."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_header_extracts_attribute(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Header annotation should extract from message.attributes."""
        received: asyncio.Queue[tuple] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-header",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(
            trace_id: Annotated[str, Header("x-trace-id")],
            data: Annotated[dict, Body()],
        ) -> None:
            await received.put((trace_id, data))

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data={"key": "value"},
                attributes={"x-trace-id": "trace-abc123"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert result[0] == "trace-abc123"
            assert result[1] == {"key": "value"}


@pytest.mark.connected
class TestSerializerPropagation:
    """Tests for serializers propagation through broker/router hierarchy."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_broker_serializer_propagates_to_subscriber(
        self,
        unique_topic: str,
        unique_subscription: str,
        project_id: str,
    ) -> None:
        """Serializer set on broker should propagate to subscribers."""
        from fastpubsub import PubSubBroker

        serializer = DefaultSerializer()
        broker = PubSubBroker(project_id=project_id, serializer=serializer)
        received: asyncio.Queue[dict] = asyncio.Queue(maxsize=1)

        @broker.subscriber(
            alias="test-serializer-prop",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(data: dict) -> None:
            await received.put(data)

        async with managed_broker(broker):
            await broker.publish(
                topic_name=unique_topic,
                data={"message": "test"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert result == {"message": "test"}

    @pytest.mark.asyncio
    async def test_content_type_automatically_added(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Content-type should be automatically added to message attributes."""
        received: asyncio.Queue[Message] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-content-type",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg)

        async with managed_broker(connected_broker):
            await connected_broker.publish(
                topic_name=unique_topic,
                data={"key": "value"},
            )
            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert "content-type" in result.attributes
            assert result.attributes["content-type"] == "application/json"


@pytest.mark.connected
class TestPydanticModels:
    """Tests for Pydantic model serialization."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_publish_pydantic_model(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        """Pydantic models should be serialized to JSON."""
        received: asyncio.Queue[UserEvent] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-pydantic",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(data: UserEvent) -> None:
            await received.put(data)

        async with managed_broker(connected_broker):
            sent_event = UserEvent(
                user_id="user123",
                action="purchase",
                data={"item": "book", "price": 29.99},
            )
            await connected_broker.publish(topic_name=unique_topic, data=sent_event)

            received_event = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert sent_event == received_event
