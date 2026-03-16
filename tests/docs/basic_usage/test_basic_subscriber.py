import pytest

from docs.snippets.basic_usage.e1_01_basic_subscriber import (
    broker,
    publish_initial_message,
)
from fastpubsub.testing import PubSubTestClient


class TestBasicSubscriber:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_after_startup_hook_forwards_successfully(self):
        async with PubSubTestClient(broker) as broker_client:
            await publish_initial_message()

            forwarded_messages = broker_client.get_published_messages()
            processing_results = broker_client.get_results()

            assert len(forwarded_messages) == 2
            assert len(processing_results) == 1

            initial_message = forwarded_messages[0]
            assert initial_message.attributes is None
            assert initial_message.topic_name == "in_topic"
            assert initial_message.project_id == "fastpubsub-pubsub-local"

            forwarded_message = forwarded_messages[1]
            assert forwarded_message.attributes is None
            assert forwarded_message.topic_name == "out_topic"
            assert forwarded_message.project_id == "fastpubsub-pubsub-local"
            assert forwarded_message.data == b"Hi!"

            processing_message = processing_results[0]
            assert processing_message.error is None
