import pytest

from docs.snippets.middlewares.e1_01_common_middlewares import (
    broker,
    publish_first_message,
)
from fastpubsub.middlewares import GZipMiddleware
from fastpubsub.testing import PubSubTestClient


class TestMiddlewaresCommon:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_gzip_middleware_successfully_executes(
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

        assert published_message.topic_name == "gzipped_topic"
        assert published_message.attributes is not None
        assert published_message.attributes["content-encoding"] == "gzip"
        assert published_message.data != b"Hi!"

        assert processed_result.error is None
        assert processed_result.message.attributes is not None
        assert (
            processed_result.message.attributes["content-encoding"] == "gzip"
        )

    @pytest.mark.docs
    def test_gzip_middleware_configuration_is_registered_with_expected_level(
        self,
    ) -> None:
        middlewares = broker.router.middlewares
        cls, _, kwargs = iter(middlewares[0])

        assert cls is GZipMiddleware
        assert kwargs == {"compresslevel": 2}
