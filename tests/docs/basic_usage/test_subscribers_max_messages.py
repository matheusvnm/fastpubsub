from unittest.mock import AsyncMock

import pytest

from docs.snippets.basic_usage.e3_02_subscribers_max_messages import (
    MAX_MESSAGES,
    broker,
    test_publish,
)
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.basic_usage.e3_02_subscribers_max_messages"


class TestSubscribersMaxMessages:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_bulk_publish_processes_messages_deterministically(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(f"{_SNIPPET}.random.randint", lambda _a, _b: 0)
        sleep = AsyncMock()
        monkeypatch.setattr(f"{_SNIPPET}.asyncio.sleep", sleep)

        async with PubSubTestClient(broker) as client:
            await test_publish()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == MAX_MESSAGES * 5
        assert all(
            message.topic_name == "test-topic"
            for message in published_messages
        )
        assert len(results) == MAX_MESSAGES * 5
        assert all(result.error is None for result in results)
        assert sleep.await_count == MAX_MESSAGES * 5
