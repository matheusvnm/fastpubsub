import pytest

from docs.snippets.basic_usage import e3_01_linked_pubsub as snippet
from fastpubsub.testing import PubSubTestClient


class TestLinkedPubsub:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_message_from_first_topic_is_chained_to_second_topic(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await snippet.test_publish()
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

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_direct_second_topic_publish_does_not_republish(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="second-topic", data={"origin": "manual"})
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "second-topic"
        assert len(results) == 1
        assert results[0].message.subscriber_name == "handle_from_another_topic"
        assert results[0].error is None
