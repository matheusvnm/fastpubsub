import httpx
import pytest

from docs.snippets.basic_usage import e6_02_custom_lifespan as snippet
from fastpubsub.testing import PubSubTestClient


class TestCustomLifespan:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_global_lifespan_creates_and_closes_http_client(self) -> None:
        client_ref: httpx.AsyncClient | None = None

        async with snippet.global_lifespan(snippet.app):
            client_ref = snippet.app.state.http_client
            assert isinstance(client_ref, httpx.AsyncClient)
            assert client_ref.is_closed is False

        assert client_ref is not None
        assert client_ref.is_closed is True

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_subscriber_processes_message_with_custom_lifespan_configuration(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="test-topic", data="hello")
            results = client.get_results()

        assert len(results) == 1
        assert results[0].message.subscriber_name == "handle_message"
        assert results[0].error is None
        assert snippet.app.lifespan_context is snippet.global_lifespan
