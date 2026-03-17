import pytest

from docs.snippets.middlewares.e4_02_broker_level_middleware import (
    broker,
    publish_first_message,
)
from fastpubsub.testing import PubSubTestClient


class TestMiddlewaresBrokerLevel:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_broker_level_middleware_applies_to_subscriber_processing(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_first_message()
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 1
        assert len(processed_results) == 1

        published_message = next(iter(published_messages))
        processed_result = next(iter(processed_results))

        assert len(published_messages) == 1
        assert published_message.topic_name == "test-topic"

        assert processed_result.error is None
        assert processed_result.message.subscriber_name == "handle_message"
