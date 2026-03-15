import pytest

from docs.snippets.basic_usage import e2_05_publish_with_attributes as snippet
from fastpubsub.testing import PubSubTestClient


class TestPublishWithAttributes:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_broker_and_publisher_calls_include_attributes(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await snippet.publish_with_broker()
            await snippet.publish_with_publisher()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert all(message.topic_name == "events" for message in published_messages)
        assert all(
            message.attributes == {"event_type": "user_login", "priority": "high"}
            for message in published_messages
        )
        assert all(result.error is None for result in results)

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_subscriber_still_processes_message_without_attributes(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"user_id": "123", "action": "logout"})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        assert results[0].message.attributes == {}
