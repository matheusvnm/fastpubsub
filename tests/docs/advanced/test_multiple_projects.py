import pytest

from docs.snippets.advanced.e1_07_multiple_projects import broker
from fastpubsub.testing import PubSubTestClient


class TestAdvancedMultipleProjects:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cross_project_publish_targets_project_b(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                topic="shared-events", data={"id": "evt-1"}, project_id="project-b"
            )
            published_messages = client.get_published_messages()

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "shared-events"
        assert published_messages[0].project_id == "project-b"
