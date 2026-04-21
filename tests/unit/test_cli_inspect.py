"""Tests for the inspect CLI command."""

import pytest
from typer.testing import CliRunner

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message

runner = CliRunner()


def _build_test_app() -> FastPubSub:
    """Build a FastPubSub app with test subscribers."""
    broker = PubSubBroker(project_id="test-project")

    @broker.subscriber(
        alias="handler_b",
        topic_name="topic-b",
        subscription_name="sub-b",
    )
    async def handler_b(message: Message) -> None:
        pass

    @broker.subscriber(
        alias="handler_a",
        topic_name="topic-a",
        subscription_name="sub-a",
        dead_letter_topic="dlt-topic",
        max_delivery_attempts=10,
        ack_deadline_seconds=30,
        filter_expression='attributes.type = "order"',
        enable_message_ordering=True,
        enable_exactly_once_delivery=True,
        max_messages=500,
        min_backoff_delay_secs=5,
        max_backoff_delay_secs=300,
        autocreate=False,
        autoupdate=True,
    )
    async def handler_a(message: Message) -> None:
        pass

    return FastPubSub(broker=broker)


def _build_empty_app() -> FastPubSub:
    """Build a FastPubSub app with no subscribers."""
    broker = PubSubBroker(project_id="test-project")
    return FastPubSub(broker=broker)


class TestSubscriberRecord:
    def test_from_subscriber_core_fields(self):
        from fastpubsub.cli.inspect import SubscriberRecord

        test_app = _build_test_app()
        subscribers = test_app.broker.router.get_subscribers()
        record = SubscriberRecord.from_subscriber(
            "handler_a", subscribers["handler_a"]
        )
        assert record.alias == "handler_a"
        assert record.project == "test-project"
        assert record.topic == "topic-a"
        assert record.subscription == "sub-a"
        assert record.handler == "handler_a"

    def test_from_subscriber_policy_fields(self):
        from fastpubsub.cli.inspect import SubscriberRecord

        test_app = _build_test_app()
        subscribers = test_app.broker.router.get_subscribers()
        record = SubscriberRecord.from_subscriber(
            "handler_a", subscribers["handler_a"]
        )
        assert record.ack_deadline == 30
        assert record.filter == 'attributes.type = "order"'
        assert record.ordering is True
        assert record.exactly_once is True
        assert record.max_messages == 500
        assert record.retry_min == 5
        assert record.retry_max == 300
        assert record.dead_letter_topic == "dlt-topic"
        assert record.dead_letter_max_attempts == 10
        assert record.autocreate is False
        assert record.autoupdate is True

    def test_from_subscriber_no_dead_letter(self):
        from fastpubsub.cli.inspect import SubscriberRecord

        test_app = _build_test_app()
        subscribers = test_app.broker.router.get_subscribers()
        record = SubscriberRecord.from_subscriber(
            "handler_b", subscribers["handler_b"]
        )
        assert record.dead_letter_topic == ""
        assert record.dead_letter_max_attempts == 0

    def test_model_dump_with_include(self):
        from fastpubsub.cli.inspect import SubscriberRecord

        test_app = _build_test_app()
        subscribers = test_app.broker.router.get_subscribers()
        record = SubscriberRecord.from_subscriber(
            "handler_a", subscribers["handler_a"]
        )
        row = record.model_dump(include={"alias", "topic"})
        assert row == {"alias": "handler_a", "topic": "topic-a"}


class TestResolveColumns:
    def test_default_columns(self):
        from fastpubsub.cli.inspect import resolve_columns

        cols = resolve_columns(None)
        assert cols == ["alias", "project", "topic", "subscription", "handler"]

    def test_columns_all(self):
        from fastpubsub.cli.inspect import (
            ALL_COLUMNS,
            resolve_columns,
        )

        cols = resolve_columns("all")
        assert cols == ALL_COLUMNS

    def test_columns_all_case_insensitive(self):
        from fastpubsub.cli.inspect import (
            ALL_COLUMNS,
            resolve_columns,
        )

        cols = resolve_columns("ALL")
        assert cols == ALL_COLUMNS

    def test_specific_columns(self):
        from fastpubsub.cli.inspect import resolve_columns

        cols = resolve_columns("alias,topic")
        assert cols == ["alias", "topic"]

    def test_columns_whitespace_tolerant(self):
        from fastpubsub.cli.inspect import resolve_columns

        cols = resolve_columns(" Alias , Topic ")
        assert cols == ["alias", "topic"]

    def test_unknown_column_raises(self):
        from fastpubsub.cli.inspect import resolve_columns

        with pytest.raises(SystemExit):
            resolve_columns("alias,nonexistent")
