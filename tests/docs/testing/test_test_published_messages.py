import pytest

from docs.snippets.testing.e1_04_test_published_messages import broker
from fastpubsub.testing import PubSubTestClient


class TestTestingPublishedMessages:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_forwarded_message_is_captured_with_expected_topic_and_data(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish("order-123", topic="incoming-orders")
            published_messages = client.get_published_messages()

        assert len(published_messages) == 2

        iterator = iter(published_messages)

        first_published_message = next(iterator)
        assert first_published_message.data == b"order-123"
        assert first_published_message.topic_name == "incoming-orders"

        second_published_message = next(iterator)
        assert second_published_message.data == b"confirmed-order-123"
        assert second_published_message.topic_name == "order-confirmations"
