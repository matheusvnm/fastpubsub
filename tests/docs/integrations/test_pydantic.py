import base64

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from docs.snippets.integrations.e1_02_pydantic import (
    FlexibleEvent,
    OrderEvent,
    StrictEvent,
    app,
    broker,
)
from fastpubsub.exceptions import Drop
from fastpubsub.testing import PubSubTestClient

client = TestClient(app)


class TestIntegrationsPydantic:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_create_order_publishes_model_and_returns_created(
        self,
    ) -> None:
        payload = {
            "order_id": "ord-1",
            "customer_id": "cust-1",
            "total": 99.9,
            "items": ["sku-1", "sku-2"],
        }

        async with PubSubTestClient(broker) as broker_client:
            response = client.post("/create-order", json=payload)
            published_messages = broker_client.get_published_messages()
            processed_results = broker_client.get_results()

        assert response.status_code == 200
        assert response.json() == {"status": "created"}

        assert len(published_messages) == 1
        assert not processed_results

        published_message = next(iter(published_messages))

        assert published_message.topic_name == "orders"
        assert (
            published_message.data.decode()
            == OrderEvent(**payload).model_dump_json()
        )

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_invalid_user_event_payload_returns_drop_error(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as broker_client:
            await broker_client.publish(
                topic="user-events", data={"user_id": "u-1"}
            )
            processed_results = broker_client.get_results()

        assert len(processed_results) == 1

        processed_result = next(iter(processed_results))

        assert isinstance(processed_result.error, Drop)
        assert "Invalid user event" in str(processed_result.error)

    @pytest.mark.docs
    def test_push_endpoint_decodes_base64_and_invokes_process_event(
        self,
    ) -> None:
        payload = {
            "order_id": "ord-2",
            "customer_id": "cust-2",
            "total": 20.0,
            "items": ["sku-3"],
        }

        event = OrderEvent(**payload)
        event_bytes = event.model_dump_json().encode()
        encoded_event = base64.b64encode(event_bytes).decode()
        push_payload = {
            "message": {
                "messageId": "msg-1",
                "data": encoded_event,
                "publishTime": "2026-02-22T00:00:00Z",
                "attributes": {"source": "test"},
            },
            "subscription": "projects/local/subscriptions/orders-sub",
        }

        response = client.post("/push-endpoint", json=push_payload)

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @pytest.mark.docs
    def test_flexible_and_strict_models_handle_extra_fields(self) -> None:
        payload = {"order_id": "ord-3", "unexpected": "value"}

        flexible = FlexibleEvent.model_validate(payload)
        assert flexible.model_dump() == {"order_id": "ord-3"}

        with pytest.raises(
            ValidationError, match="Extra inputs are not permitted"
        ):
            StrictEvent.model_validate(payload)
