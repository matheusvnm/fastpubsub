"""Tests for CLI wildcard subscriber snippet."""

from __future__ import annotations

import pytest

from docs.snippets.basic_usage.e8_03_cli_wildcards import broker
from fastpubsub._internal.selector import SubscriberSelector


@pytest.mark.docs
class TestCLIWildcardSnippet:
    """Verify the wildcard snippet registers expected aliases."""

    def test_all_aliases_registered(self) -> None:
        subscribers = broker.router.get_subscribers()
        assert "orders.process" in subscribers
        assert "orders.validate" in subscribers
        assert "orders.notify" in subscribers
        assert "payments.process" in subscribers

    def test_wildcard_orders_star(self) -> None:
        subscribers = broker.router.get_subscribers()
        selector = SubscriberSelector(patterns={"orders.*"})
        result = selector.select(subscribers)
        assert len(result) == 3

    def test_wildcard_star_process(self) -> None:
        subscribers = broker.router.get_subscribers()
        selector = SubscriberSelector(patterns={"*.process"})
        result = selector.select(subscribers)
        assert len(result) == 2

    def test_wildcard_double_star_process(self) -> None:
        subscribers = broker.router.get_subscribers()
        selector = SubscriberSelector(patterns={"**.process"})
        result = selector.select(subscribers)
        assert len(result) == 2
