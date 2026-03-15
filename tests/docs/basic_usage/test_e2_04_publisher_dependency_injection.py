from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from docs.snippets.basic_usage import e2_04_publisher_dependency_injection as snippet
from fastpubsub.testing import PubSubTestClient

client = TestClient(snippet.app)


class TestPublisherDependencyInjection:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_new_user_endpoint_runs_use_case_and_publishes_event(self) -> None:
        async with PubSubTestClient(snippet.broker) as broker_client:
            response = client.post("/new-user", json={"name": "Alice", "age": 22})
            published_messages = broker_client.get_published_messages()
            results = broker_client.get_results()

        assert response.status_code == 200
        assert response.json() == {"message": "Use case executed successfully"}

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "new-users-topic"
        assert published_messages[0].data == b'{"name":"Alice","age":22}'
        assert published_messages[0].project_id == "fastpubsub-pubsub-local"

        assert len(results) == 1
        assert results[0].error is None
        assert results[0].message.subscriber_name == "handle_user_event"

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_use_case_delegates_publish_to_injected_publisher(self) -> None:
        publisher = AsyncMock()
        publisher.publish = AsyncMock(return_value="msg-id-1")
        use_case = snippet.MyAwesomeUseCase(publisher=publisher)

        result = await use_case.execute({"name": "Bob", "age": 30})

        assert result == "msg-id-1"
        publisher.publish.assert_awaited_once_with(data={"name": "Bob", "age": 30})
