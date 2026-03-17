from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced.e1_03_filters import broker
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.advanced.e1_03_filters"


class TestAdvancedFilters:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_filtered_and_unfiltered_subscribers_route_expected_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process_order = AsyncMock()
        process_user_event = AsyncMock()
        log_to_audit_trail = AsyncMock()
        monkeypatch.setattr(f"{_SNIPPET}.process_order", process_order)
        monkeypatch.setattr(
            f"{_SNIPPET}.process_user_event", process_user_event
        )
        monkeypatch.setattr(
            f"{_SNIPPET}.log_to_audit_trail", log_to_audit_trail
        )

        async with PubSubTestClient(broker) as client:
            await client.publish(
                topic="multi-events",
                data="order",
                attributes={"event_type": "order"},
            )
            await client.publish(
                topic="multi-events",
                data="user",
                attributes={"event_type": "user"},
            )
            results = client.get_results()

        names = [result.message.subscriber_name for result in results]
        assert names.count("handle_order_events") == 1
        assert names.count("handle_users") == 1
        assert names.count("audit_all_events") == 2
        assert process_order.await_count == 1
        assert process_user_event.await_count == 1
        assert log_to_audit_trail.await_count == 2

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_subscriber_does_not_receive_unfiltered_message(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(topic="events", data="ignored")
            results = client.get_results()

        assert results == []
