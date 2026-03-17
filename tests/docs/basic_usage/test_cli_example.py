import pytest

from docs.snippets.basic_usage.e8_01_cli_example import broker
from fastpubsub.testing import PubSubTestClient


class TestCliExampleSnippet:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_subscriber_listen_order_and_notification_topics(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(topic="orders", data={"id": "ord-1"})
            await client.publish(topic="notifications", data={"id": "notif-1"})
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert [message.topic_name for message in published_messages] == [
            "orders",
            "notifications",
        ]
        assert len(results) == 2
        assert [result.message.subscriber_name for result in results] == [
            "handle_orders",
            "handle_notifications",
        ]
        assert all(result.error is None for result in results)
