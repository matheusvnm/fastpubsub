import pytest

from docs.snippets.basic_usage.e6_01_lifespan_hooks import (
    announce_startup,
    broker,
)
from fastpubsub.testing import PubSubTestClient


class TestLifespanHooks:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_after_startup_hook_publishes_system_online_message(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await announce_startup()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "system-logs"
        assert published_messages[0].data == b'{"status":"online"}'
        assert len(results) == 1
        assert results[0].message.subscriber_name == "handle_system_log"
        assert results[0].error is None
