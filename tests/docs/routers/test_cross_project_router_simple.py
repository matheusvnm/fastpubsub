import pytest

from docs.snippets.routers import e1_04_cross_project_router_simple as snippet
from fastpubsub.testing import PubSubTestClient


class TestRoutersCrossProjectRouterSimple:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cross_project_router_only_consumes_messages_for_router_project(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"event": "default-project"})
            await client.publish(
                topic="events", data={"event": "project-b"}, project_id="project-b"
            )
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 2
        assert len(processed_results) == 1

        processed_result = next(iter(processed_results))
        project_ids = [message.project_id for message in published_messages]

        assert project_ids == [
            "fastpubsub-pubsub-local",
            "project-b",
        ]

        assert processed_result.error is None
        assert processed_result.message.subscriber_name == "handle_external_event"
        assert processed_result.message.project_id == "project-b"
