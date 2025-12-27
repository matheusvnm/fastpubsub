"""Integration tests for message publishing."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

from fastpubsub import Message
from tests.integration.conftest import managed_broker

if TYPE_CHECKING:

    from fastpubsub.broker import PubSubBroker


@pytest.mark.connected
class TestMessagePublishing:
    """Test message publishing with real PubSub emulator."""

    timeout: float = 15.0

    @pytest.mark.asyncio
    async def test_publish_byte_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-string-pub",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg.data)

        async with managed_broker(connected_broker):
            sent_data = b"Some bytes to deliver"
            await connected_broker.publish(
                topic_name=unique_topic,
                data=sent_data,
            )

            received_data = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert received_data == sent_data

    @pytest.mark.asyncio
    async def test_publish_string_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-string-pub",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg.data)

        async with managed_broker(connected_broker):
            sent_data = "Test string message"
            await connected_broker.publish(
                topic_name=unique_topic,
                data=sent_data,
            )

            received_data = await asyncio.wait_for(received.get(), timeout=self.timeout)
            assert received_data.decode() == sent_data

    @pytest.mark.asyncio
    async def test_publish_dict_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-dict-pub",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg.data)

        async with managed_broker(connected_broker):
            sent_data = {"key": "value", "number": 42}
            await connected_broker.publish(
                topic_name=unique_topic,
                data=sent_data,
            )

            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            received_data = json.loads(result.decode())
            assert sent_data == received_data

    @pytest.mark.asyncio
    async def test_publish_with_attributes(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[Message] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-attr-pub",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg)

        async with managed_broker(connected_broker):
            sent_attributes = {"source": "test", "version": "1.0"}
            sent_data = b"Hello world!"

            await connected_broker.publish(
                topic_name=unique_topic,
                data=sent_data,
                attributes=sent_attributes,
            )

            result = await asyncio.wait_for(received.get(), timeout=self.timeout)

            assert isinstance(result, Message)
            assert result.data == sent_data
            assert result.attributes == sent_attributes

    @pytest.mark.asyncio
    async def test_publish_pydantic_message(
        self,
        unique_topic: str,
        unique_subscription: str,
        connected_broker: PubSubBroker,
    ) -> None:
        received: asyncio.Queue[bytes] = asyncio.Queue(maxsize=1)

        @connected_broker.subscriber(
            alias="test-dict-pub",
            topic_name=unique_topic,
            subscription_name=unique_subscription,
            autocreate=True,
        )
        async def handler(msg: Message) -> None:
            await received.put(msg.data)

        async with managed_broker(connected_broker):

            class SomePydanticModel(BaseModel):
                attribute_name: str
                attribute_value: str

            sent_data = SomePydanticModel(attribute_name="car_color", attribute_value="blue")
            await connected_broker.publish(
                topic_name=unique_topic,
                data=sent_data,
            )

            result = await asyncio.wait_for(received.get(), timeout=self.timeout)
            received_data = SomePydanticModel.model_validate_json(result)
            assert sent_data == received_data
