import pytest

from docs.snippets.basic_usage.e2_01_basic_publisher import (
    broker as first_broker,
)
from docs.snippets.basic_usage.e2_01_basic_publisher import (
    publish_first_message as first_publish_message,
)
from docs.snippets.basic_usage.e2_02_basic_publisher import (
    broker as second_broker,
)
from docs.snippets.basic_usage.e2_02_basic_publisher import (
    publish_first_message as second_publish_message,
)
from fastpubsub import PubSubBroker
from fastpubsub.testing import PubSubTestClient
from fastpubsub.types import NoArgAsyncCallable


class TestBasicPublisherBrokerPublish:
    @pytest.mark.parametrize(
        "broker,after_startup_call",
        [
            (
                first_broker,
                first_publish_message,
            ),
            (
                second_broker,
                second_publish_message,
            ),
        ],
    )
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_after_startup_publish_sends_message_to_topic(
        self, broker: PubSubBroker, after_startup_call: NoArgAsyncCallable
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await after_startup_call()
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 1
        assert len(processed_results) == 1

        published_message = next(iter(published_messages))
        processed_result = next(iter(processed_results))

        assert published_message.topic_name == "test-topic"
        assert published_message.project_id == "fastpubsub-pubsub-local"
        assert published_message.data == b'{"hello":"world"}'
        assert published_message.attributes is None

        assert processed_result.message.subscriber_name == "handle"
        assert processed_result.error is None
