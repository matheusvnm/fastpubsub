import base64
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from docs.snippets.basic_usage.e1_02_push_subscriber import app

client = TestClient(app)


class TestPushSubscription:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_push_subscription_message_successfully(self):
        message_content = {
            "messageId": "1",
            "data": base64.b64encode(b"some_info").decode(),
            "publishTime": str(datetime.now(tz=UTC)),
            "attributes": {"key": "value"},
        }

        payload = {
            "message": message_content,
            "subscription": "projects/abc/subscriptions/awesome_sub",
        }

        response = client.post("/push-handler", json=payload)
        response_content = response.json()

        assert response_content["status"] == "ok"
        assert response_content["processed_data"] == message_content["data"]
        assert response_content["processed_attributes"] == message_content["attributes"]
