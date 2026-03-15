import pytest

from docs.snippets.basic_usage import e2_07_cross_project_publish as snippet
from fastpubsub.testing import PubSubTestClient


class TestCrossProjectPublish:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cross_project_publish_targets_configured_project(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await snippet.publish_cross_project_broker()
            await snippet.publish_cross_project_publisher()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert all(message.topic_name == "shared-events" for message in published_messages)
        assert all(
            message.project_id == "fastpubsub-pubsub-local" for message in published_messages
        )
        assert results == []

        subscribers = snippet.broker.router._get_subscribers()
        assert subscribers["shared-events-handler"].project_id == "other-project-id"
        assert snippet.cross_project_publisher.project_id == "other-project-id"

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_default_project_message_is_not_delivered_to_cross_project_subscriber(
        self,
    ) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="shared-events", data={"event": "local-only"})
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 1
        assert published_messages[0].project_id == "fastpubsub-pubsub-local"
        assert results == []
