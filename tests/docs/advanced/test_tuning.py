from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced.e1_06_tuning import broker
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.advanced.e1_06_tuning"


class TestAdvancedTuning:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_high_throughput_handler_processes_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fast_async_operation = AsyncMock()
        monkeypatch.setattr(f"{_SNIPPET}.fast_async_operation", fast_async_operation)

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="high-events", data={"value": 1})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        fast_async_operation.assert_awaited_once()
