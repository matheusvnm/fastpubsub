from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced.e1_05_delivery_guarantees import broker
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.advanced.e1_05_delivery_guarantees"


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

        monkeypatch.setattr(f"{_SNIPPET}.process_event", process_event)
        monkeypatch.setattr(f"{_SNIPPET}.mark_as_processed", mark_as_processed)
        monkeypatch.setattr(
            f"{_SNIPPET}.is_already_processed", _is_already_processed
        )
        mark_as_processed.side_effect = _mark_as_processed

        async with PubSubTestClient(broker) as client:
            await client.publish(
                topic="events",
                data={"event": 1},
                attributes={"event_id": "e-1"},
            )
            await client.publish(
                topic="events",
                data={"event": 1},
                attributes={"event_id": "e-1"},
            )
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
        monkeypatch.setattr(f"{_SNIPPET}.process_event", process_event)

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="events", data={"event": 1})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        process_event.assert_not_awaited()
