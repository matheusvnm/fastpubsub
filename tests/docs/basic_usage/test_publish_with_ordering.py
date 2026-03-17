import pytest

from docs.snippets.basic_usage.e2_06_publish_with_ordering import (
    broker,
    publish_with_broker,
    publish_with_publisher,
)
from fastpubsub.testing import PubSubTestClient


class TestPublishWithOrdering:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_ordering_key_is_forwarded_for_broker_and_publisher(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_with_broker()
            await publish_with_publisher()
            published_messages = client.get_published_messages()
            assert client._mock_client is not None
            publish_calls = client._mock_client.publish.await_args_list
            results = client.get_results()

        assert len(published_messages) == 4
        assert all(
            message.topic_name == "user-events"
            for message in published_messages
        )
        assert [call.kwargs["ordering_key"] for call in publish_calls] == [
            "user-123",
            "user-123",
            "user-123",
            "user-123",
        ]
        assert all(
            call.kwargs["attributes"] == {"user_id": "user-123"}
            for call in publish_calls
        )
        assert all(result.error is None for result in results)
