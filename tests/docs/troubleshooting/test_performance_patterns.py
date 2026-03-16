import json
from unittest.mock import AsyncMock

import pytest

from docs.snippets.troubleshooting.e1_02_performance_patterns import broker
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.troubleshooting.e1_02_performance_patterns"


class TestTroubleshootingPerformancePatterns:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_performance_handlers_process_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fast_operation = AsyncMock()
        process_in_order = AsyncMock()
        monkeypatch.setattr(f"{_SNIPPET}.fast_operation", fast_operation)
        monkeypatch.setattr(f"{_SNIPPET}.process_in_order", process_in_order)

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="high-throughput", data={"batch": 1})
            await client.publish(
                topic="ordered-events",
                data={"action": "rebuild"},
                attributes={"user_id": "user-42"},
            )
            results = client.get_results()

        assert len(results) == 2
        assert all(result.error is None for result in results)
        fast_operation.assert_awaited_once()
        process_in_order.assert_awaited_once()
        assert json.loads(fast_operation.await_args.args[0]) == {"batch": 1}
        ordered_args = process_in_order.await_args.args
        assert ordered_args[0] == "user-42"
        assert json.loads(ordered_args[1]) == {"action": "rebuild"}
