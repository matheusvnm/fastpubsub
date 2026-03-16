import pytest
from fastapi.testclient import TestClient

from docs.snippets.basic_usage.e2_04_publisher_dependency_injection import app, broker
from fastpubsub.testing import PubSubTestClient

client = TestClient(app)


class TestPublisherDependencyInjection:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_new_user_endpoint_runs_use_case_and_publishes_event(self) -> None:
        async with PubSubTestClient(broker) as broker_client:
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
