"""Tests for SubscriberSelector."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from fastpubsub._internal.selector import SubscriberSelector
from fastpubsub.pubsub.subscriber import Subscriber


def _make_subscriber(name: str) -> MagicMock:
    """Create a mock subscriber with a given name."""
    mock = MagicMock(spec=Subscriber)
    mock.name = name
    return mock


class TestSubscriberSelectorExactMatch:
    """Tests for exact alias matching."""

    def test_empty_patterns_returns_all(self) -> None:
        subs = {
            "a": _make_subscriber("a"),
            "b": _make_subscriber("b"),
        }
        selector = SubscriberSelector(patterns=set())
        result = selector.select(subs)
        assert len(result) == 2

    def test_exact_match_single(self) -> None:
        subs = {
            "a": _make_subscriber("a"),
            "b": _make_subscriber("b"),
        }
        selector = SubscriberSelector(patterns={"a"})
        result = selector.select(subs)
        assert len(result) == 1
        assert result[0].name == "a"

    def test_exact_match_multiple(self) -> None:
        subs = {
            "a": _make_subscriber("a"),
            "b": _make_subscriber("b"),
        }
        selector = SubscriberSelector(patterns={"a", "b"})
        result = selector.select(subs)
        assert len(result) == 2

    def test_exact_match_not_found_logs_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        subs = {"a": _make_subscriber("a")}
        selector = SubscriberSelector(patterns={"a", "nonexistent"})
        fps_logger = logging.getLogger("fastpubsub")
        fps_logger.propagate = True
        try:
            with caplog.at_level(logging.WARNING):
                result = selector.select(subs)
        finally:
            fps_logger.propagate = False
        assert len(result) == 1
        assert "nonexistent" in caplog.text

    def test_no_match_at_all_returns_empty(self) -> None:
        subs = {"a": _make_subscriber("a")}
        selector = SubscriberSelector(patterns={"x"})
        result = selector.select(subs)
        assert len(result) == 0

    def test_case_insensitive(self) -> None:
        subs = {"orders.process": _make_subscriber("process")}
        selector = SubscriberSelector(patterns={"Orders.Process"})
        result = selector.select(subs)
        assert len(result) == 1

    def test_case_insensitive_mixed_case_alias(self) -> None:
        subs = {"Orders.Process": _make_subscriber("process")}
        selector = SubscriberSelector(patterns={"orders.*"})
        result = selector.select(subs)
        assert len(result) == 1
        assert result[0].name == "process"

    def test_no_duplicates(self) -> None:
        subs = {"a": _make_subscriber("a")}
        selector = SubscriberSelector(patterns={"a"})
        result = selector.select(subs)
        assert len(result) == 1


class TestSubscriberSelectorGlobMatch:
    """Tests for single-star and question-mark glob patterns."""

    @pytest.fixture
    def subscribers(self) -> dict[str, MagicMock]:
        return {
            "orders.process": _make_subscriber("process"),
            "orders.validate": _make_subscriber("validate"),
            "orders.notify": _make_subscriber("notify"),
            "payments.process": _make_subscriber("pay_process"),
            "payments.refund": _make_subscriber("refund"),
        }

    def test_star_matches_within_segment(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"orders.*"})
        result = selector.select(subscribers)
        assert len(result) == 3
        names = {s.name for s in result}
        assert names == {"process", "validate", "notify"}

    def test_star_does_not_cross_segments(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        subscribers["orders.br.process"] = _make_subscriber("br_process")
        selector = SubscriberSelector(patterns={"orders.*"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert "br_process" not in names

    def test_question_mark(self, subscribers: dict[str, MagicMock]) -> None:
        selector = SubscriberSelector(patterns={"orders.proc?ss"})
        result = selector.select(subscribers)
        assert len(result) == 1
        assert result[0].name == "process"

    def test_star_prefix_match(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"*.process"})
        result = selector.select(subscribers)
        assert len(result) == 2
        names = {s.name for s in result}
        assert names == {"process", "pay_process"}


class TestSubscriberSelectorDoubleStarMatch:
    """Tests for ** hierarchical glob patterns."""

    @pytest.fixture
    def subscribers(self) -> dict[str, MagicMock]:
        return {
            "process": _make_subscriber("root_process"),
            "orders.process": _make_subscriber("orders_process"),
            "v1.orders.process": _make_subscriber("v1_process"),
            "v1.orders.br.process": _make_subscriber("v1_br_process"),
            "orders.validate": _make_subscriber("validate"),
            "payments.process": _make_subscriber("pay_process"),
        }

    def test_double_star_suffix(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"orders.**"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {"orders_process", "validate"}

    def test_double_star_prefix(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"**.process"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {
            "root_process",
            "orders_process",
            "v1_process",
            "v1_br_process",
            "pay_process",
        }

    def test_double_star_middle(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"v1.**.process"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {"v1_process", "v1_br_process"}

    def test_double_star_composed(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"**.orders.*.process"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {"v1_br_process"}

    def test_double_star_zero_segments(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"**.orders.process"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert "orders_process" in names
        assert "v1_process" in names

    def test_multiple_patterns_union(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"orders.*", "payments.*"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {
            "orders_process",
            "validate",
            "pay_process",
        }

    def test_consecutive_double_stars_collapsed(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"**.**.process"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {
            "root_process",
            "orders_process",
            "v1_process",
            "v1_br_process",
            "pay_process",
        }

    def test_mixed_exact_and_glob(
        self, subscribers: dict[str, MagicMock]
    ) -> None:
        selector = SubscriberSelector(patterns={"process", "orders.*"})
        result = selector.select(subscribers)
        names = {s.name for s in result}
        assert names == {
            "root_process",
            "orders_process",
            "validate",
        }
