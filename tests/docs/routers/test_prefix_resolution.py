import pytest

from docs.snippets.routers.e1_01_prefix_resolution import broker
from fastpubsub.testing import PubSubTestClient


class TestRoutersPrefixResolution:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_empty_prefix_routers_route_message_with_unique_aliases(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(topic="test-router-topic", data={"hello": "world"})
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 1
        assert len(processed_results) == 2

        published_message = next(iter(published_messages))
        subscribers = {result.message.subscriber_name for result in processed_results}

        assert published_message.topic_name == "test-router-topic"
        assert subscribers == {
            "unnamed_router_handler",
            "other_unnamed_router_handler",
        }
        assert all(result.error is None for result in processed_results)
