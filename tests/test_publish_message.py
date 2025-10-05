from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from fastpubsub.broker import PubSubBroker
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter


class TestPublishMessage:
    @pytest.mark.asyncio
    async def test_publish_with_all_fields_successfully(
        self, router_a: PubSubRouter, broker: PubSubBroker, publisher: Publisher
    ):
        data = {"data": b"data", "ordering_key": "key", "attributes": {"attr1": "val1"}}

        with patch.object(
            Publisher, "build_callstack", return_value=AsyncMock()
        ) as mock_build_callstack:
            await publisher.publish(**data)
            await router_a.publish(topic_name="topic", **data)
            await broker.publish(topic_name="topic", **data)

            callable_instance = mock_build_callstack.return_value
            assert callable_instance.on_publish.call_count == 3
            callable_instance.on_publish.assert_called_with(**data)

    @pytest.mark.parametrize("topic_name", [None, True, 101])
    def test_publisher_with_invalid_topic_name_raises_exception(
        self, topic_name: str, broker: PubSubBroker, router_a: PubSubRouter
    ):
        broker.include_router(router_a)
        with pytest.raises(ValidationError):
            broker.publisher(topic_name=topic_name)

        with pytest.raises(ValidationError):
            router_a.publisher(topic_name=topic_name)

    @pytest.mark.parametrize(
        "data",
        [
            {"data": True},
            {"data": 101, "ordering_key": "key"},
            {"data": "text", "ordering_key": {}, "attributes": {"key": "value"}},
            {"data": "text", "attributes": {"key": object}},
            {"data": "text", "autocreate": None},
        ],
    )
    @pytest.mark.asyncio
    async def test_publish_with_invalid_fields_raises_exception(
        self,
        data: dict[str, Any],
        broker: PubSubBroker,
        router_a: PubSubRouter,
        publisher: Publisher,
    ):
        with pytest.raises(ValidationError):
            await publisher.publish(**data)

        with pytest.raises(ValidationError):
            await broker.publish(topic_name="a", **data)

        with pytest.raises(ValidationError):
            await router_a.publish(topic_name="a", **data)
