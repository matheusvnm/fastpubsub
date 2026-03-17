import pytest

from docs.snippets.basic_usage.e2_07_cross_project_publish import (
    broker,
    cross_project_publisher,
    publish_cross_project_broker,
    publish_cross_project_publisher,
)
from fastpubsub.testing import PubSubTestClient


class TestCrossProjectPublish:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cross_project_publish_targets_configured_project(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_cross_project_broker()
            await publish_cross_project_publisher()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert all(
            message.topic_name == "shared-events"
            for message in published_messages
        )
        assert all(
            message.project_id == "fastpubsub-pubsub-local"
            for message in published_messages
        )
        assert results == []

        subscribers = broker.router._get_subscribers()
        assert (
            subscribers["shared-events-handler"].project_id
            == "other-project-id"
        )
        assert cross_project_publisher.project_id == "other-project-id"
