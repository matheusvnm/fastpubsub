import pytest

from docs.snippets.routers.multi_domain_routers.main import (
    broker,
    publish_test_messages,
)
from fastpubsub.testing import PubSubTestClient


class TestRoutersMultiDomainMain:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_broker_routes_messages_to_domain_handlers(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_test_messages()

            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 2
        assert len(processed_results) == 2

        topics = [result.message.topic_name for result in processed_results]
        subscribers = {
            result.message.subscriber_name for result in processed_results
        }

        assert topics == ["users-topic", "posts-topic"]
        assert subscribers == {"handle_user_message", "handle_post_message"}
        assert all(result.error is None for result in processed_results)
