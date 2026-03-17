from http.client import UNPROCESSABLE_ENTITY

import pytest
from fastapi.testclient import TestClient

from docs.snippets.integrations.e1_01_fastapi import UserTask, app, broker
from fastpubsub.testing import PubSubTestClient

client = TestClient(app)


class TestIntegrationsFastapi:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_tasks_endpoint_and_related_routes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async with PubSubTestClient(broker) as broker_client:
            task_response = client.post(
                "/tasks/", json={"user_id": 10, "task_name": "ship"}
            )
            order_response = client.post(
                "/new-orders/", json={"product_id": "p-1", "quantity": 2}
            )
            status_response = client.get("/api/v1/status")

            published_messages = broker_client.get_published_messages()
            processed_results = broker_client.get_results()

        assert task_response.status_code == 200
        assert task_response.json() == {"message": "Task accepted"}

        assert order_response.status_code == 200
        assert order_response.json() == {
            "order_id": "order-123",
            "status": "created",
        }

        assert status_response.status_code == 200
        assert status_response.json() == {"status": "healthy"}

        assert len(published_messages) == 1
        assert len(processed_results) == 1

        processed_message = next(iter(published_messages))
        processed_result = next(iter(processed_results))

        assert processed_message.topic_name == "tasks"

        assert processed_result.error is None
        assert processed_result.message.topic_name == "tasks"
        assert (
            processed_result.message.data.decode()
            == UserTask(user_id=10, task_name="ship").model_dump_json()
        )

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_orders_endpoint_rejects_non_positive_quantity(self) -> None:
        async with PubSubTestClient(broker) as broker_client:
            response = client.post(
                "/orders/", json={"product_id": "p-1", "quantity": 0}
            )
            published_messages = broker_client.get_published_messages()

        assert not published_messages
        assert response.status_code == UNPROCESSABLE_ENTITY
        assert any(
            error["loc"][-1] == "quantity"
            for error in response.json()["detail"]
        )
