from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced import e1_03_filters as snippet
from fastpubsub.testing import PubSubTestClient


class TestAdvancedFilters:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_filtered_and_unfiltered_subscribers_route_expected_messages(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process_order = AsyncMock()
        process_user_event = AsyncMock()
        log_to_audit_trail = AsyncMock()
        monkeypatch.setattr(snippet, "process_order", process_order)
        monkeypatch.setattr(snippet, "process_user_event", process_user_event)
        monkeypatch.setattr(snippet, "log_to_audit_trail", log_to_audit_trail)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(
                topic="multi-events", data="order", attributes={"event_type": "order"}
            )
            await client.publish(
                topic="multi-events", data="user", attributes={"event_type": "user"}
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
    async def test_filtered_subscriber_does_not_receive_message_without_attributes(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data="ignored")
            results = client.get_results()

        assert results == []

    @pytest.mark.docs
    def test_filter_expressions_are_kept_in_subscriber_configuration(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()

        assert (
            subscribers["order-handler"].delivery_policy.filter_expression
            == 'attributes.event_type = "order"'
        )
        assert (
            subscribers["critical-alerts"].delivery_policy.filter_expression
            == 'attributes.severity = "critical" OR attributes.severity = "high"'
        )
        assert subscribers["audit-handler"].delivery_policy.filter_expression == ""
