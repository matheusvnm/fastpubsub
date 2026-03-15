import pytest

from docs.snippets.advanced import e1_07_multiple_projects as snippet
from fastpubsub.testing import PubSubTestClient


class TestAdvancedMultipleProjects:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cross_project_publish_targets_project_b(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(
                topic="shared-events", data={"id": "evt-1"}, project_id="project-b"
            )
            published_messages = client.get_published_messages()

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "shared-events"
        assert published_messages[0].project_id == "project-b"

    @pytest.mark.docs
    def test_nested_router_project_id_propagation_keeps_inner_override(self) -> None:
        subscribers = snippet.nested_broker.router._get_subscribers()
        nested = subscribers["external.analytics.handler"]

        assert nested.project_id == "project-c"
        assert nested.subscription_name == "external.analytics.metrics-subscription"

    @pytest.mark.docs
    def test_cross_project_subscriber_config_is_preserved(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()
        local = subscribers["local-handler"]
        cross_project = subscribers["cross-project-handler"]
        router_subscriber = subscribers["external.shared-handler"]

        assert local.project_id == "project-a"
        assert cross_project.project_id == "project-b"
        assert router_subscriber.project_id == "project-b"
