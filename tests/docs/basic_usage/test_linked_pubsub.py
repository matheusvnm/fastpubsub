import pytest

from docs.snippets.basic_usage.e3_01_linked_pubsub import broker, test_publish
from fastpubsub.testing import PubSubTestClient


class TestLinkedPubsub:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_message_from_first_topic_is_chained_to_second_topic(self) -> None:
        async with PubSubTestClient(broker) as client:
            await test_publish()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 2
        assert published_messages[0].topic_name == "first-topic"
        assert published_messages[0].data == b'{"hello":"world"}'
        assert published_messages[1].topic_name == "second-topic"
        assert published_messages[1].data == b'{"foo":"bar"}'
        assert {result.message.subscriber_name for result in results} == {
            "handle",
            "handle_from_another_topic",
        }
        assert all(result.error is None for result in results)
