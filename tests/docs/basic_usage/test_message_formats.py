import pytest

from docs.snippets.basic_usage.e2_03_message_formats import (
    broker,
    publish_initial_messages,
)
from fastpubsub.testing import PubSubTestClient


class TestMessageFormats:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_publish_supports_pydantic_dict_string_and_bytes(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_initial_messages()
            published_messages = client.get_published_messages()
            processed_messages = client.get_results()

        assert len(published_messages) == 4
        assert published_messages[0].data == (
            b'{"event":"checkout","source":"checkout-cart",'
            b'"message":"the user put a item to the cart"}'
        )
        assert published_messages[1].data == b'{"some_dict":"dict_data"}'
        assert published_messages[2].data == b"some_string_text"
        assert published_messages[3].data == b"some_byte_text"

        assert all(
            message.topic_name == "test-topic-pydantic"
            for message in published_messages
        )
        assert all(result.error is None for result in processed_messages)
