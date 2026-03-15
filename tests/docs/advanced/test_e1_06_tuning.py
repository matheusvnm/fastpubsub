from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced import e1_06_tuning as snippet
from fastpubsub.middlewares import GZipMiddleware
from fastpubsub.testing import PubSubTestClient


class TestAdvancedTuning:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_high_throughput_handler_processes_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fast_async_operation = AsyncMock()
        monkeypatch.setattr(snippet, "fast_async_operation", fast_async_operation)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="high-events", data={"value": 1})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        fast_async_operation.assert_awaited_once()

    @pytest.mark.docs
    def test_subscriber_tuning_values_are_configured(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()

        assert subscribers["high-throughput"].control_flow_policy.max_messages == 500
        assert subscribers["slow-processor"].delivery_policy.ack_deadline_seconds == 600
        assert subscribers["slow-processor"].control_flow_policy.max_messages == 10
        assert subscribers["network-sensitive"].retry_policy.min_backoff_delay_secs == 5
        assert subscribers["network-sensitive"].retry_policy.max_backoff_delay_secs == 60
        assert subscribers["external-api"].retry_policy.min_backoff_delay_secs == 30
        assert subscribers["external-api"].retry_policy.max_backoff_delay_secs == 600

    @pytest.mark.docs
    def test_tuned_broker_shutdown_timeout_and_middleware(self) -> None:
        subscribers = snippet.tuned_broker.router._get_subscribers()
        optimized = subscribers["optimized-processor"]
        middlewares = snippet.tuned_broker.router.middlewares

        assert snippet.tuned_broker.shutdown_timeout == 30.0
        assert optimized.control_flow_policy.max_messages == 200
        assert optimized.delivery_policy.ack_deadline_seconds == 120
        assert optimized.retry_policy.min_backoff_delay_secs == 10
        assert optimized.retry_policy.max_backoff_delay_secs == 300
        assert len(middlewares) == 1
        assert middlewares[0].cls is GZipMiddleware
        assert middlewares[0].kwargs == {"compresslevel": 6}
