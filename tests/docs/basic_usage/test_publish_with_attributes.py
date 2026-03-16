import pytest

from docs.snippets.basic_usage.e2_05_publish_with_attributes import (
    broker,
    publish_with_broker,
    publish_with_publisher,
)
from fastpubsub.testing import PubSubTestClient


class TestPublishWithAttributes:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_broker_and_publisher_calls_include_attributes(self) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_with_broker()
            await publish_with_publisher()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert all(message.topic_name == "events" for message in published_messages)
        assert all(
            message.attributes == {"event_type": "user_login", "priority": "high"}
            for message in published_messages
        )
        assert all(result.error is None for result in results)
