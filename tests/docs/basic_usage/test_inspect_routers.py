"""Tests for inspect routers snippet."""

from __future__ import annotations

import pytest

from docs.snippets.basic_usage.e9_02_inspect_routers import broker


@pytest.mark.docs
class TestInspectRoutersSnippet:
    """Verify the inspect routers snippet registers expected aliases."""

    def test_all_aliases_registered(self) -> None:
        subscribers = broker.router.get_subscribers()
        assert "platform.orders.fulfill" in subscribers
        assert "platform.orders.invoice" in subscribers
        assert "platform.analytics.track" in subscribers

    def test_subscriber_count(self) -> None:
        subscribers = broker.router.get_subscribers()
        assert len(subscribers) == 3

    def test_track_uses_different_project(self) -> None:
        subscribers = broker.router.get_subscribers()
        sub = subscribers["platform.analytics.track"]
        assert sub.project_id == "analytics-warehouse"

    def test_order_subscribers_share_topic(self) -> None:
        subscribers = broker.router.get_subscribers()
        fulfill = subscribers["platform.orders.fulfill"]
        invoice = subscribers["platform.orders.invoice"]
        assert fulfill.topic_name == "order-events"
        assert invoice.topic_name == "order-events"
