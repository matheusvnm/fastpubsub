from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced import e1_05_delivery_guarantees as snippet
from fastpubsub.testing import PubSubTestClient


class TestAdvancedDeliveryGuarantees:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_idempotent_handler_skips_duplicate_events(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process_event = AsyncMock()
        mark_as_processed = AsyncMock()
        processed_ids: set[str] = set()

        async def _is_already_processed(event_id: str) -> bool:
            return event_id in processed_ids

        async def _mark_as_processed(event_id: str) -> None:
            processed_ids.add(event_id)

        monkeypatch.setattr(snippet, "process_event", process_event)
        monkeypatch.setattr(snippet, "mark_as_processed", mark_as_processed)
        monkeypatch.setattr(snippet, "is_already_processed", _is_already_processed)
        mark_as_processed.side_effect = _mark_as_processed

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"event": 1}, attributes={"event_id": "e-1"})
            await client.publish(topic="events", data={"event": 1}, attributes={"event_id": "e-1"})
            results = client.get_results()

        assert len(results) == 2
        assert process_event.await_count == 1
        assert mark_as_processed.await_count == 1

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_idempotent_handler_skips_messages_without_event_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process_event = AsyncMock()
        monkeypatch.setattr(snippet, "process_event", process_event)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"event": 1})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        process_event.assert_not_awaited()

    @pytest.mark.docs
    def test_exactly_once_and_retry_delivery_configuration_values(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()
        payment = subscribers["payment-processor"]
        critical = subscribers["critical-payment"]

        assert payment.delivery_policy.enable_exactly_once_delivery is True
        assert critical.delivery_policy.enable_exactly_once_delivery is True
        assert critical.retry_policy.min_backoff_delay_secs == 10
        assert critical.retry_policy.max_backoff_delay_secs == 300
        assert critical.dead_letter_policy is not None
        assert critical.dead_letter_policy.max_delivery_attempts == 5
