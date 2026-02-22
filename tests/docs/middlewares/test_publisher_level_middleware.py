import pytest

from docs.snippets.middlewares.e4_05_publisher_level_middleware import broker, publish_with_trace
from fastpubsub.testing import PubSubTestClient


class TestMiddlewaresPublisherLevel:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_publisher_level_middleware_mutates_attributes_for_custom_publisher(self) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_with_trace()
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 1
        assert len(processed_results) == 1

        published_message = next(iter(published_messages))
        processed_result = next(iter(processed_results))

        assert published_message.topic_name == "events"
        assert published_message.attributes is not None
        assert published_message.attributes["x-trace-id"] == "generated-trace-id"

        assert processed_result.error is None
        assert processed_result.message.attributes is not None
        assert processed_result.message.attributes["x-trace-id"] == "generated-trace-id"
