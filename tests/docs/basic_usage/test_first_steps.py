import pytest
from fastapi.testclient import TestClient

from docs.snippets.basic_usage.e0_01_first_steps import Address, app, broker
from fastpubsub.testing import PubSubTestClient

client = TestClient(app)


class TestFirstSteps:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_publish_forwards_successfully(self):
        address = Address(street="Rua das Flores", number="321")
        payload = address.model_dump()

        async with PubSubTestClient(broker) as broker_client:
            response = client.post("/addresses", json=payload)

            response_content = response.json()
            assert response_content == {"message": "Address published"}

            forwarded_messages = broker_client.get_published_messages()
            processed_messages = broker_client.get_results()
            assert len(forwarded_messages) == 1
            assert len(processed_messages) == 1

            forwarded_message = forwarded_messages[0]
            assert forwarded_message.attributes is None
            assert forwarded_message.topic_name == "address-events"
            assert forwarded_message.project_id == "your-project-id"
            assert forwarded_message.data.decode() == address.model_dump_json()

            processed_message = processed_messages[0]
            assert processed_message.error is None
            assert processed_message.return_value == {"status": "ok"}
            assert processed_message.message.data == forwarded_message.data
