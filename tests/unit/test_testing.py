"""Unit tests for the testing module."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.middlewares import Middleware
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.testing import (
    ProcessingResult,
    PublishedMessage,
    PubSubTestClient,
    matches_filter,
)


class TestMatchesFilter:
    """Tests for the filter expression evaluation function."""

    # Empty/None filter tests
    def test_empty_filter_matches_all(self):
        assert matches_filter({"key": "value"}, "") is True
        assert matches_filter({}, "") is True

    def test_whitespace_filter_matches_all(self):
        assert matches_filter({"key": "value"}, "   ") is True
        assert matches_filter({"key": "value"}, "\n\t") is True

    def test_exact_match_equals_double_quotes(self):
        assert matches_filter({"event_type": "order"}, 'attributes.event_type = "order"') is True
        assert matches_filter({"event_type": "user"}, 'attributes.event_type = "order"') is False

    def test_exact_match_equals_single_quotes(self):
        assert matches_filter({"type": "test"}, "attributes.type = 'test'") is True
        assert matches_filter({"type": "other"}, "attributes.type = 'test'") is False

    def test_exact_match_missing_attribute(self):
        assert matches_filter({}, 'attributes.event_type = "order"') is False

    def test_exact_match_empty_value(self):
        assert matches_filter({"key": ""}, 'attributes.key = ""') is True
        assert matches_filter({"key": "value"}, 'attributes.key = ""') is False

    def test_not_equals_different_value(self):
        assert matches_filter({"type": "user"}, 'attributes.type != "order"') is True

    def test_not_equals_same_value(self):
        assert matches_filter({"type": "order"}, 'attributes.type != "order"') is False

    def test_not_equals_missing_attribute(self):
        assert matches_filter({}, 'attributes.type != "order"') is True

    def test_attribute_exists(self):
        assert matches_filter({"key": "any"}, "attributes:key") is True

    def test_attribute_not_exists(self):
        assert matches_filter({}, "attributes:key") is False
        assert matches_filter({"other": "value"}, "attributes:key") is False

    def test_attribute_exists_empty_value(self):
        assert matches_filter({"key": ""}, "attributes:key") is True

    def test_attribute_exists_with_hyphen_in_name(self):
        assert matches_filter({"my-key": "value"}, "attributes:my-key") is True

    def test_attribute_exists_with_underscore_in_name(self):
        assert matches_filter({"my_key": "value"}, "attributes:my_key") is True

    def test_has_prefix_match(self):
        result = matches_filter({"region": "us-east-1"}, 'hasPrefix(attributes.region, "us-")')
        assert result is True

    def test_has_prefix_no_match(self):
        result = matches_filter({"region": "eu-west-1"}, 'hasPrefix(attributes.region, "us-")')
        assert result is False

    def test_has_prefix_missing_attribute(self):
        assert matches_filter({}, 'hasPrefix(attributes.region, "us-")') is False

    def test_has_prefix_empty_prefix(self):
        assert matches_filter({"region": "us-east-1"}, 'hasPrefix(attributes.region, "")') is True

    def test_has_prefix_exact_match(self):
        assert matches_filter({"region": "us"}, 'hasPrefix(attributes.region, "us")') is True

    def test_and_both_true(self):
        attrs = {"type": "order", "status": "pending"}
        expr = 'attributes.type = "order" AND attributes.status = "pending"'
        assert matches_filter(attrs, expr) is True

    def test_and_first_false(self):
        attrs = {"type": "user", "status": "pending"}
        expr = 'attributes.type = "order" AND attributes.status = "pending"'
        assert matches_filter(attrs, expr) is False

    def test_and_second_false(self):
        attrs = {"type": "order", "status": "completed"}
        expr = 'attributes.type = "order" AND attributes.status = "pending"'
        assert matches_filter(attrs, expr) is False

    def test_and_both_false(self):
        attrs = {"type": "user", "status": "completed"}
        expr = 'attributes.type = "order" AND attributes.status = "pending"'
        assert matches_filter(attrs, expr) is False

    def test_multiple_and(self):
        attrs = {"a": "1", "b": "2", "c": "3"}
        expr = 'attributes.a = "1" AND attributes.b = "2" AND attributes.c = "3"'
        assert matches_filter(attrs, expr) is True

    def test_or_first_true(self):
        attrs = {"type": "order"}
        expr = 'attributes.type = "order" OR attributes.type = "user"'
        assert matches_filter(attrs, expr) is True

    def test_or_second_true(self):
        attrs = {"type": "user"}
        expr = 'attributes.type = "order" OR attributes.type = "user"'
        assert matches_filter(attrs, expr) is True

    def test_or_both_true(self):
        attrs = {"type": "order"}
        expr = 'attributes.type = "order" OR attributes:type'
        assert matches_filter(attrs, expr) is True

    def test_or_both_false(self):
        attrs = {"type": "product"}
        expr = 'attributes.type = "order" OR attributes.type = "user"'
        assert matches_filter(attrs, expr) is False

    def test_multiple_or(self):
        attrs = {"type": "refund"}
        expr = 'attributes.type = "order" OR attributes.type = "user" OR attributes.type = "refund"'
        assert matches_filter(attrs, expr) is True

    def test_not_operator_false_becomes_true(self):
        assert matches_filter({"type": "user"}, 'NOT attributes.type = "order"') is True

    def test_not_operator_true_becomes_false(self):
        assert matches_filter({"type": "order"}, 'NOT attributes.type = "order"') is False

    def test_not_with_existence(self):
        assert matches_filter({}, "NOT attributes:key") is True
        assert matches_filter({"key": "value"}, "NOT attributes:key") is False

    def test_double_not(self):
        assert matches_filter({"type": "order"}, 'NOT NOT attributes.type = "order"') is True

    def test_and_has_higher_precedence_than_or(self):
        """Tests if 'a OR b AND c' means 'a OR (b AND c)'"""
        attrs = {"a": "1"}
        expr = 'attributes.a = "1" OR attributes.b = "2" AND attributes.c = "3"'

        # True OR (False AND False) -> True OR False -> True
        assert matches_filter(attrs, expr) is True

    def test_and_has_higher_precedence_than_or_second_case(self):
        attrs = {"b": "2", "c": "3"}
        expr = 'attributes.a = "1" OR attributes.b = "2" AND attributes.c = "3"'
        # False OR (True AND True) -> False OR True -> True
        assert matches_filter(attrs, expr) is True

    def test_precedence_with_explicit_parentheses(self):
        """Tests if parantheses change the precendence"""

        attrs = {"a": "1", "b": "2"}
        expr = '(attributes.a = "1" OR attributes.b = "2") AND attributes.c = "3"'
        # (True OR True) AND False -> True AND False -> False
        assert matches_filter(attrs, expr) is False

    def test_parentheses_grouping(self):
        attrs = {"type": "order", "priority": "high"}
        expr = (
            '(attributes.type = "order" OR attributes.type = "user") '
            'AND attributes.priority = "high"'
        )
        assert matches_filter(attrs, expr) is True

    def test_nested_parentheses(self):
        attrs = {"a": "1"}
        expr = '((attributes.a = "1"))'
        assert matches_filter(attrs, expr) is True

    def test_parentheses_with_not(self):
        attrs = {"type": "order"}
        expr = 'NOT (attributes.type = "user")'
        assert matches_filter(attrs, expr) is True

    def test_complex_expression(self):
        attrs = {"type": "order", "priority": "high", "region": "us-east-1"}
        expr = (
            '(attributes.type = "order" OR attributes.type = "refund") '
            'AND attributes.priority = "high" '
            'AND hasPrefix(attributes.region, "us-")'
        )
        assert matches_filter(attrs, expr) is True

    def test_complex_expression_not_matching(self):
        attrs = {"type": "user", "priority": "high", "region": "us-east-1"}
        expr = (
            '(attributes.type = "order" OR attributes.type = "refund") '
            'AND attributes.priority = "high" '
            'AND hasPrefix(attributes.region, "us-")'
        )
        assert matches_filter(attrs, expr) is False

    def test_invalid_expression_unclosed_string(self):
        with pytest.raises(ValueError, match="Unclosed string"):
            matches_filter({}, 'attributes.key = "unclosed')

    def test_invalid_expression_unexpected_character(self):
        with pytest.raises(ValueError, match="Unexpected character"):
            matches_filter({}, "attributes.key @ value")

    def test_invalid_expression_missing_value(self):
        with pytest.raises(ValueError, match="Expected STRING"):
            matches_filter({}, "attributes.key = ")

    def test_invalid_expression_missing_operator(self):
        with pytest.raises(ValueError, match="Expected = or !="):
            matches_filter({}, 'attributes.key "value"')

    def test_invalid_expression_unclosed_parenthesis(self):
        with pytest.raises(ValueError, match="Expected RPAREN"):
            matches_filter({}, '(attributes.key = "value"')

    def test_invalid_expression_unexpected_token_at_end(self):
        with pytest.raises(ValueError, match="Unexpected token"):
            matches_filter({}, 'attributes.key = "value" extra')


class TestPubSubTestClient:
    """Tests for the PubSubTestClient class."""

    @pytest.fixture
    def broker(self) -> PubSubBroker:
        return PubSubBroker(project_id="test-project")

    @pytest.fixture
    def mock_handler(self) -> MagicMock:
        return MagicMock()

    # Initialization tests
    def test_init_with_broker(self, broker: PubSubBroker):
        client = PubSubTestClient(broker)
        assert client.broker is broker
        assert client._published_messages == []
        assert client._patchers == []

    def test_init_accepts_kwargs(self, broker: PubSubBroker):
        client = PubSubTestClient(broker, some_option=True)
        assert client.broker is broker

    # Context manager tests
    @pytest.mark.asyncio
    async def test_context_manager_enter_exit(self, broker: PubSubBroker):
        client = PubSubTestClient(broker)

        async with client as ctx:
            assert ctx is client
            assert len(client._patchers) > 0

        assert len(client._patchers) == 0

    @pytest.mark.asyncio
    async def test_context_manager_stops_patches_on_exception(self, broker: PubSubBroker):
        client = PubSubTestClient(broker)

        with pytest.raises(RuntimeError):
            async with client:
                raise RuntimeError("Test error")

        assert len(client._patchers) == 0

    # Publish without subscribers
    @pytest.mark.asyncio
    async def test_publish_without_subscribers(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("test data", topic="test-topic")

            messages = client.get_published_messages()
            assert len(messages) == 1
            assert messages[0].topic_name == "test-topic"

    # Publish with single subscriber
    @pytest.mark.asyncio
    async def test_publish_with_single_subscriber(self, broker: PubSubBroker):
        received_messages: list[Message] = []

        @broker.subscriber(
            alias="test-sub", topic_name="test-topic", subscription_name="test-subscription"
        )
        async def handler(msg: Message) -> None:
            received_messages.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish({"key": "value"}, topic="test-topic")

        assert len(received_messages) == 1

    # Publish with multiple subscribers (same topic)
    @pytest.mark.asyncio
    async def test_publish_with_multiple_subscribers_same_topic(self, broker: PubSubBroker):
        received_a: list[Message] = []
        received_b: list[Message] = []

        @broker.subscriber(
            alias="sub-a", topic_name="test-topic", subscription_name="subscription-a"
        )
        async def handler_a(msg: Message) -> None:
            received_a.append(msg)

        @broker.subscriber(
            alias="sub-b", topic_name="test-topic", subscription_name="subscription-b"
        )
        async def handler_b(msg: Message) -> None:
            received_b.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="test-topic")

        assert len(received_a) == 1
        assert len(received_b) == 1

    # Publish routes to correct topic
    @pytest.mark.asyncio
    async def test_publish_routes_to_correct_topic(self, broker: PubSubBroker):
        received_a: list[Message] = []
        received_b: list[Message] = []

        @broker.subscriber(alias="sub-a", topic_name="topic-a", subscription_name="subscription-a")
        async def handler_a(msg: Message) -> None:
            received_a.append(msg)

        @broker.subscriber(alias="sub-b", topic_name="topic-b", subscription_name="subscription-b")
        async def handler_b(msg: Message) -> None:
            received_b.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic-a")

        assert len(received_a) == 1
        assert len(received_b) == 0

    # Filter expression tests
    @pytest.mark.asyncio
    async def test_filter_expression_exact_match(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="filtered-sub",
            topic_name="events",
            subscription_name="filtered-subscription",
            filter_expression='attributes.event_type = "order"',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            # Should be received
            await client.publish(
                {"data": "order1"}, topic="events", attributes={"event_type": "order"}
            )
            # Should NOT be received
            await client.publish(
                {"data": "user1"}, topic="events", attributes={"event_type": "user"}
            )

        assert len(received) == 1
        assert received[0].attributes["event_type"] == "order"

    @pytest.mark.asyncio
    async def test_filter_expression_no_filter_receives_all(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="unfiltered-sub", topic_name="events", subscription_name="unfiltered-subscription"
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events", attributes={"type": "a"})
            await client.publish("msg2", topic="events", attributes={"type": "b"})

        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_filter_expression_and_operator(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="and-filter-sub",
            topic_name="events",
            subscription_name="and-filter-subscription",
            filter_expression='attributes.type = "order" AND attributes.status = "pending"',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            # Matches
            await client.publish(
                "msg1", topic="events", attributes={"type": "order", "status": "pending"}
            )
            # Doesn't match (wrong status)
            await client.publish(
                "msg2", topic="events", attributes={"type": "order", "status": "completed"}
            )

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_filter_expression_or_operator(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="or-filter-sub",
            topic_name="events",
            subscription_name="or-filter-subscription",
            filter_expression='attributes.type = "order" OR attributes.type = "refund"',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events", attributes={"type": "order"})
            await client.publish("msg2", topic="events", attributes={"type": "refund"})
            await client.publish("msg3", topic="events", attributes={"type": "user"})

        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_filter_expression_not_operator(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="not-filter-sub",
            topic_name="events",
            subscription_name="not-filter-subscription",
            filter_expression='NOT attributes.type = "test"',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events", attributes={"type": "order"})
            await client.publish("msg2", topic="events", attributes={"type": "test"})

        assert len(received) == 1
        assert received[0].attributes["type"] == "order"

    @pytest.mark.asyncio
    async def test_filter_expression_existence_check(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="exists-filter-sub",
            topic_name="events",
            subscription_name="exists-filter-subscription",
            filter_expression="attributes:priority",
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events", attributes={"priority": "high"})
            await client.publish("msg2", topic="events", attributes={"other": "value"})

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_filter_expression_has_prefix(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="prefix-filter-sub",
            topic_name="events",
            subscription_name="prefix-filter-subscription",
            filter_expression='hasPrefix(attributes.region, "us-")',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events", attributes={"region": "us-east-1"})
            await client.publish("msg2", topic="events", attributes={"region": "eu-west-1"})

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_filter_expression_multiple_subscribers_different_filters(
        self, broker: PubSubBroker
    ):
        orders: list[Message] = []
        users: list[Message] = []

        @broker.subscriber(
            alias="orders-sub",
            topic_name="events",
            subscription_name="orders-subscription",
            filter_expression='attributes.type = "order"',
        )
        async def orders_handler(msg: Message) -> None:
            orders.append(msg)

        @broker.subscriber(
            alias="users-sub",
            topic_name="events",
            subscription_name="users-subscription",
            filter_expression='attributes.type = "user"',
        )
        async def users_handler(msg: Message) -> None:
            users.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("o1", topic="events", attributes={"type": "order"})
            await client.publish("u1", topic="events", attributes={"type": "user"})
            await client.publish("o2", topic="events", attributes={"type": "order"})

        assert len(orders) == 2
        assert len(users) == 1

    # Message inspection tests
    @pytest.mark.asyncio
    async def test_get_published_messages(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="topic-a", attributes={"key": "1"})
            await client.publish("msg2", topic="topic-b", attributes={"key": "2"})

            messages = client.get_published_messages()
            assert len(messages) == 2
            assert messages[0].topic_name == "topic-a"
            assert messages[1].topic_name == "topic-b"

    @pytest.mark.asyncio
    async def test_get_published_messages_returns_copy(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("msg", topic="topic")

            messages1 = client.get_published_messages()
            messages2 = client.get_published_messages()

            assert messages1 is not messages2
            assert messages1 == messages2

    @pytest.mark.asyncio
    async def test_clear_published_messages(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="topic")
            await client.publish("msg2", topic="topic")

            assert len(client.get_published_messages()) == 2

            client.clear_published_messages()

            assert len(client.get_published_messages()) == 0

    # Middleware execution tests
    @pytest.mark.asyncio
    async def test_middleware_execution_in_test_client(self, broker: PubSubBroker):
        middleware_calls: list[str] = []

        class TestMiddleware(BaseMiddleware):
            async def on_message(self, message: Message):
                middleware_calls.append("middleware")
                return await super().on_message(message)

        @broker.subscriber(
            alias="middleware-sub",
            topic_name="test-topic",
            subscription_name="middleware-subscription",
            middlewares=[Middleware(TestMiddleware)],
        )
        async def handler(msg: Message) -> None:
            middleware_calls.append("handler")

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="test-topic")

        assert middleware_calls == ["middleware", "handler"]

    # Message content tests
    @pytest.mark.asyncio
    async def test_message_attributes_passed_correctly(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(
            alias="attr-sub", topic_name="test-topic", subscription_name="attr-subscription"
        )
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish(
                "test", topic="test-topic", attributes={"key1": "value1", "key2": "value2"}
            )

        assert received_msg is not None
        assert received_msg.attributes == {"key1": "value1", "key2": "value2"}

    @pytest.mark.asyncio
    async def test_message_topic_and_subscriber_name(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(
            alias="meta-sub", topic_name="my-topic", subscription_name="my-subscription"
        )
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="my-topic")

        assert received_msg is not None
        assert received_msg.topic_name == "my-topic"
        assert received_msg.subscriber_name == "handler"
        assert received_msg.project_id == "test-project"

    @pytest.mark.asyncio
    async def test_message_has_unique_id(self, broker: PubSubBroker):
        received_msgs: list[Message] = []

        @broker.subscriber(alias="id-sub", topic_name="topic", subscription_name="subscription")
        async def handler(msg: Message) -> None:
            received_msgs.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="topic")
            await client.publish("msg2", topic="topic")

        assert len(received_msgs) == 2
        assert received_msgs[0].id != received_msgs[1].id
        assert received_msgs[0].id.startswith("test-msg-")
        assert received_msgs[1].id.startswith("test-msg-")

    @pytest.mark.asyncio
    async def test_message_delivery_attempt_is_one(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(
            alias="delivery-sub", topic_name="topic", subscription_name="subscription"
        )
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic")

        assert received_msg is not None
        assert received_msg.delivery_attempt == 1

    # Data serialization tests
    @pytest.mark.asyncio
    async def test_publish_dict_data(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(alias="dict-sub", topic_name="topic", subscription_name="subscription")
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish({"key": "value", "number": 123}, topic="topic")

        assert received_msg is not None
        assert b'"key"' in received_msg.data
        assert b'"value"' in received_msg.data

    @pytest.mark.asyncio
    async def test_publish_string_data(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(alias="str-sub", topic_name="topic", subscription_name="subscription")
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish("hello world", topic="topic")

        assert received_msg is not None
        assert received_msg.data == b"hello world"

    @pytest.mark.asyncio
    async def test_publish_bytes_data(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(alias="bytes-sub", topic_name="topic", subscription_name="subscription")
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish(b"raw bytes", topic="topic")

        assert received_msg is not None
        assert received_msg.data == b"raw bytes"

    # No attributes tests
    @pytest.mark.asyncio
    async def test_publish_without_attributes(self, broker: PubSubBroker):
        received_msg: Message | None = None

        @broker.subscriber(
            alias="no-attr-sub", topic_name="topic", subscription_name="subscription"
        )
        async def handler(msg: Message) -> None:
            nonlocal received_msg
            received_msg = msg

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic")

        assert received_msg is not None
        assert received_msg.attributes == {}

    @pytest.mark.asyncio
    async def test_filter_with_no_attributes_published(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="filter-sub",
            topic_name="topic",
            subscription_name="subscription",
            filter_expression='attributes.type = "order"',
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic")  # No attributes

        assert len(received) == 0  # Should not match

    # Published message dataclass tests
    @pytest.mark.asyncio
    async def test_published_message_has_project_id(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic")

            messages = client.get_published_messages()
            assert len(messages) == 1
            assert isinstance(messages[0], PublishedMessage)
            assert messages[0].project_id == "test-project"

    @pytest.mark.asyncio
    async def test_published_message_with_explicit_project_id(self, broker: PubSubBroker):
        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="topic", project_id="other-project")

            messages = client.get_published_messages()
            assert len(messages) == 1
            assert messages[0].project_id == "other-project"

    # Cross-project tests
    @pytest.mark.asyncio
    async def test_cross_project_publish_matches_subscriber(self, broker: PubSubBroker):
        received: list[Message] = []

        @broker.subscriber(
            alias="cross-sub",
            topic_name="events",
            subscription_name="cross-subscription",
            project_id="other-project",
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events", project_id="other-project")

        assert len(received) == 1
        assert received[0].project_id == "other-project"

    @pytest.mark.asyncio
    async def test_default_project_does_not_match_cross_project_subscriber(
        self, broker: PubSubBroker
    ):
        received: list[Message] = []

        @broker.subscriber(
            alias="cross-sub2",
            topic_name="events",
            subscription_name="cross-subscription2",
            project_id="other-project",
        )
        async def handler(msg: Message) -> None:
            received.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")  # defaults to broker project

        assert len(received) == 0

    @pytest.mark.asyncio
    async def test_same_topic_different_projects_isolated(self, broker: PubSubBroker):
        received_default: list[Message] = []
        received_other: list[Message] = []

        @broker.subscriber(
            alias="default-sub",
            topic_name="events",
            subscription_name="default-subscription",
        )
        async def default_handler(msg: Message) -> None:
            received_default.append(msg)

        @broker.subscriber(
            alias="other-sub",
            topic_name="events",
            subscription_name="other-subscription",
            project_id="other-project",
        )
        async def other_handler(msg: Message) -> None:
            received_other.append(msg)

        async with PubSubTestClient(broker) as client:
            await client.publish("msg1", topic="events")  # default project
            await client.publish("msg2", topic="events", project_id="other-project")

        assert len(received_default) == 1
        assert len(received_other) == 1

    # Processing results tests
    @pytest.mark.asyncio
    async def test_processing_result_basic(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="result-sub",
            topic_name="events",
            subscription_name="result-subscription",
        )
        async def handler(msg: Message) -> None:
            pass

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")

            results = client.get_results()
            assert len(results) == 1
            assert isinstance(results[0], ProcessingResult)
            assert results[0].message.subscriber_name == "handler"
            assert results[0].message.topic_name == "events"
            assert results[0].message.project_id == "test-project"
            assert results[0].error is None

    @pytest.mark.asyncio
    async def test_processing_result_return_value(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="return-sub",
            topic_name="events",
            subscription_name="return-subscription",
        )
        async def handler(msg: Message) -> str:
            return "processed"

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")

            results = client.get_results()
            assert len(results) == 1
            assert results[0].return_value == "processed"

    @pytest.mark.asyncio
    async def test_processing_result_error_capture(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="error-sub",
            topic_name="events",
            subscription_name="error-subscription",
        )
        async def failing_handler(msg: Message) -> None:
            raise ValueError("handler failed")

        @broker.subscriber(
            alias="ok-sub",
            topic_name="events",
            subscription_name="ok-subscription",
        )
        async def ok_handler(msg: Message) -> str:
            return "ok"

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")

            results = client.get_results()
            assert len(results) == 2

            error_results = [r for r in results if r.error is not None]
            ok_results = [r for r in results if r.error is None]

            assert len(error_results) == 1
            assert isinstance(error_results[0].error, ValueError)
            assert str(error_results[0].error) == "handler failed"

            assert len(ok_results) == 1
            assert ok_results[0].return_value == "ok"

    @pytest.mark.asyncio
    async def test_processing_results_multiple_subscribers(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="sub-a2", topic_name="events", subscription_name="sub-a2-subscription"
        )
        async def handler_a(msg: Message) -> None:
            pass

        @broker.subscriber(
            alias="sub-b2", topic_name="events", subscription_name="sub-b2-subscription"
        )
        async def handler_b(msg: Message) -> None:
            pass

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")

            results = client.get_results()
            assert len(results) == 2
            names = {r.message.subscriber_name for r in results}
            assert names == {"handler_a", "handler_b"}

    @pytest.mark.asyncio
    async def test_processing_results_cross_project(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="proj-default-sub",
            topic_name="events",
            subscription_name="proj-default-subscription",
        )
        async def default_handler(msg: Message) -> None:
            pass

        @broker.subscriber(
            alias="proj-other-sub",
            topic_name="events",
            subscription_name="proj-other-subscription",
            project_id="other-project",
        )
        async def other_handler(msg: Message) -> None:
            pass

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")
            await client.publish("test", topic="events", project_id="other-project")

            results = client.get_results()
            assert len(results) == 2
            projects = {r.message.project_id for r in results}
            assert projects == {"test-project", "other-project"}

    @pytest.mark.asyncio
    async def test_processing_results_returns_copy(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="copy-sub", topic_name="events", subscription_name="copy-subscription"
        )
        async def handler(msg: Message) -> None:
            pass

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")

            results1 = client.get_results()
            results2 = client.get_results()
            assert results1 is not results2
            assert results1 == results2

    @pytest.mark.asyncio
    async def test_clear_processing_results(self, broker: PubSubBroker):
        @broker.subscriber(
            alias="clear-sub",
            topic_name="events",
            subscription_name="clear-subscription",
        )
        async def handler(msg: Message) -> None:
            pass

        async with PubSubTestClient(broker) as client:
            await client.publish("test", topic="events")
            assert len(client.get_results()) == 1

            client.clear_results()
            assert len(client.get_results()) == 0
