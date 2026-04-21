"""Tests for inspect basic snippet."""

from __future__ import annotations

import pytest

from docs.snippets.basic_usage.e9_01_inspect_basic import broker


@pytest.mark.docs
class TestInspectBasicSnippet:
    """Verify the inspect basic snippet registers expected aliases."""

    def test_all_aliases_registered(self) -> None:
        subscribers = broker.router.get_subscribers()
        assert "process-orders" in subscribers
        assert "charge-payments" in subscribers
        assert "send-notifications" in subscribers

    def test_subscriber_count(self) -> None:
        subscribers = broker.router.get_subscribers()
        assert len(subscribers) == 3

    def test_process_orders_has_dead_letter(self) -> None:
        subscribers = broker.router.get_subscribers()
        sub = subscribers["process-orders"]
        assert sub.dead_letter_policy is not None
        assert sub.dead_letter_policy.topic_name == "order-events-dlq"

    def test_charge_payments_has_exactly_once(self) -> None:
        subscribers = broker.router.get_subscribers()
        sub = subscribers["charge-payments"]
        assert sub.delivery_policy.enable_exactly_once_delivery is True

    def test_send_notifications_has_filter(self) -> None:
        subscribers = broker.router.get_subscribers()
        sub = subscribers["send-notifications"]
        assert (
            sub.delivery_policy.filter_expression
            == 'attributes.channel = "email"'
        )
