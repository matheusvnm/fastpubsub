from unittest.mock import AsyncMock

import pytest

from docs.snippets.basic_usage import e3_02_subscribers_max_messages as snippet
from fastpubsub.testing import PubSubTestClient


class TestSubscribersMaxMessages:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_bulk_publish_processes_messages_deterministically(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(snippet.random, "randint", lambda _a, _b: 0)
        sleep = AsyncMock()
        monkeypatch.setattr(snippet.asyncio, "sleep", sleep)

        async with PubSubTestClient(snippet.broker) as client:
            await snippet.test_publish()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == snippet.MAX_MESSAGES * 5
        assert all(message.topic_name == "test-topic" for message in published_messages)
        assert len(results) == snippet.MAX_MESSAGES * 5
        assert all(result.error is None for result in results)
        assert sleep.await_count == snippet.MAX_MESSAGES * 5

    @pytest.mark.docs
    def test_subscriber_control_flow_uses_documented_max_messages(self) -> None:
        subscriber = snippet.broker.router._get_subscribers()["test-alias"]

        assert snippet.MAX_MESSAGES == 10
        assert subscriber.control_flow_policy.max_messages == snippet.MAX_MESSAGES
