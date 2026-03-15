import json
from unittest.mock import AsyncMock

import pytest

from docs.snippets.troubleshooting import e1_02_performance_patterns as snippet
from fastpubsub.testing import PubSubTestClient


class TestTroubleshootingPerformancePatterns:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_performance_handlers_process_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fast_operation = AsyncMock()
        process_in_order = AsyncMock()
        monkeypatch.setattr(snippet, "fast_operation", fast_operation)
        monkeypatch.setattr(snippet, "process_in_order", process_in_order)

        async with PubSubTestClient(snippet.broker) as client:
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

    @pytest.mark.docs
    def test_performance_tuning_configuration_values(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()
        high_throughput = subscribers["high-throughput-handler"]
        low_memory = subscribers["low-memory-handler"]
        ordered = subscribers["ordered-handler"]
        middlewares = snippet.broker.router.middlewares

        assert snippet.broker.shutdown_timeout == 30.0
        assert high_throughput.control_flow_policy.max_messages == 500
        assert low_memory.control_flow_policy.max_messages == 10
        assert ordered.delivery_policy.enable_message_ordering is True
        assert len(middlewares) == 1
        assert middlewares[0].cls is snippet.ProfilingMiddleware
