import pytest

from docs.snippets.middlewares.e4_01_full_logging_middleware import broker, publish_first_message
from fastpubsub.testing import PubSubTestClient


class TestMiddlewaresFullLogging:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_full_logging_middleware_adds_trace_id_and_processes_message(self) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_first_message()
            published_messages = client.get_published_messages()
            processed_results = client.get_results()

        assert len(published_messages) == 1
        assert len(processed_results) == 1

        published_message = next(iter(published_messages))
        processed_result = next(iter(processed_results))

        assert published_message.topic_name == "test-topic"
        assert published_message.attributes is not None
        assert published_message.attributes["x-trace-id"] == "some-trace-id"
        assert processed_result.error is None
