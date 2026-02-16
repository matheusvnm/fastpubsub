"""Testing utilities for FastPubSub."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastpubsub.datastructures import Message
from fastpubsub.pubsub import Publisher

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker

__all__ = ["PubSubTestClient", "PublishedMessage", "ProcessingResult", "matches_filter"]


@dataclass(frozen=True)
class PublishedMessage:
    """Record of a message published during testing."""

    topic_name: str
    data: bytes
    attributes: dict[str, str] | None
    project_id: str


@dataclass(frozen=True)
class ProcessingResult:
    """Record of a subscriber handler invocation during testing.

    The ``message`` field already carries ``subscriber_name``, ``topic_name``
    and ``project_id``, so they are accessible via ``result.message.*``.
    """

    message: Message
    return_value: Any
    error: BaseException | None = None


# =============================================================================
# Filter Expression Evaluation
# =============================================================================


class _TokenType(Enum):
    """Token types for filter expression parsing."""

    AND = auto()
    OR = auto()
    NOT = auto()
    EQUALS = auto()
    NOT_EQUALS = auto()
    LPAREN = auto()
    RPAREN = auto()
    COMMA = auto()
    STRING = auto()
    IDENTIFIER = auto()
    ATTRIBUTES_DOT = auto()
    ATTRIBUTES_COLON = auto()
    HAS_PREFIX = auto()
    EOF = auto()


@dataclass
class _Token:
    """A token from the filter expression."""

    type: _TokenType
    value: str


def _tokenize(expression: str) -> list[_Token]:
    """Tokenize a filter expression.

    Args:
        expression: The filter expression string.

    Returns:
        List of tokens.

    Raises:
        ValueError: If the expression contains invalid tokens.
    """
    tokens: list[_Token] = []
    pos = 0
    length = len(expression)

    while pos < length:
        # Skip whitespace
        if expression[pos].isspace():
            pos += 1
            continue

        # Check for keywords and operators
        remaining = expression[pos:]

        # Logical operators (must check before identifiers)
        if remaining.startswith("AND") and (len(remaining) == 3 or not remaining[3].isalnum()):
            tokens.append(_Token(_TokenType.AND, "AND"))
            pos += 3
            continue

        if remaining.startswith("OR") and (len(remaining) == 2 or not remaining[2].isalnum()):
            tokens.append(_Token(_TokenType.OR, "OR"))
            pos += 2
            continue

        if remaining.startswith("NOT") and (len(remaining) == 3 or not remaining[3].isalnum()):
            tokens.append(_Token(_TokenType.NOT, "NOT"))
            pos += 3
            continue

        # hasPrefix function
        if remaining.startswith("hasPrefix"):
            tokens.append(_Token(_TokenType.HAS_PREFIX, "hasPrefix"))
            pos += 9
            continue

        # attributes. prefix (for comparisons)
        if remaining.startswith("attributes."):
            tokens.append(_Token(_TokenType.ATTRIBUTES_DOT, "attributes."))
            pos += 11
            continue

        # attributes: prefix (for existence check)
        if remaining.startswith("attributes:"):
            tokens.append(_Token(_TokenType.ATTRIBUTES_COLON, "attributes:"))
            pos += 11
            continue

        # Comparison operators
        if remaining.startswith("!="):
            tokens.append(_Token(_TokenType.NOT_EQUALS, "!="))
            pos += 2
            continue

        if remaining.startswith("="):
            tokens.append(_Token(_TokenType.EQUALS, "="))
            pos += 1
            continue

        # Parentheses and comma
        if expression[pos] == "(":
            tokens.append(_Token(_TokenType.LPAREN, "("))
            pos += 1
            continue

        if expression[pos] == ")":
            tokens.append(_Token(_TokenType.RPAREN, ")"))
            pos += 1
            continue

        if expression[pos] == ",":
            tokens.append(_Token(_TokenType.COMMA, ","))
            pos += 1
            continue

        # String literals (double or single quotes)
        if expression[pos] in ('"', "'"):
            quote_char = expression[pos]
            end_pos = pos + 1
            while end_pos < length and expression[end_pos] != quote_char:
                end_pos += 1
            if end_pos >= length:
                raise ValueError(f"Unclosed string literal at position {pos}")
            string_value = expression[pos + 1 : end_pos]
            tokens.append(_Token(_TokenType.STRING, string_value))
            pos = end_pos + 1
            continue

        # Identifiers (attribute names)
        if expression[pos].isalpha() or expression[pos] == "_":
            end_pos = pos
            while end_pos < length and (
                expression[end_pos].isalnum() or expression[end_pos] in ("_", "-")
            ):
                end_pos += 1
            identifier = expression[pos:end_pos]
            tokens.append(_Token(_TokenType.IDENTIFIER, identifier))
            pos = end_pos
            continue

        raise ValueError(f"Unexpected character '{expression[pos]}' at position {pos}")

    tokens.append(_Token(_TokenType.EOF, ""))
    return tokens


class _FilterParser:
    """Recursive descent parser for Pub/Sub filter expressions.

    Supports:
    - attributes.key = "value" (exact match)
    - attributes.key != "value" (not equal)
    - attributes:key (existence check)
    - hasPrefix(attributes.key, "prefix") (prefix match)
    - AND, OR, NOT logical operators
    - Parentheses for grouping
    """

    def __init__(self, tokens: list[_Token], attributes: dict[str, str]) -> None:
        """Initialize the parser.

        Args:
            tokens: List of tokens from tokenization.
            attributes: Message attributes to evaluate against.
        """
        self.tokens = tokens
        self.attributes = attributes
        self.pos = 0

    def parse(self) -> bool:
        """Parse and evaluate the expression.

        Returns:
            True if attributes match the filter, False otherwise.

        Raises:
            ValueError: If the expression is invalid.
        """
        result = self._or_expr()
        if self._current().type != _TokenType.EOF:
            raise ValueError(f"Unexpected token: {self._current().value}")
        return result

    def _current(self) -> _Token:
        """Get the current token."""
        return self.tokens[self.pos]

    def _advance(self) -> _Token:
        """Advance to the next token and return the current one."""
        token = self.tokens[self.pos]
        if self.pos < len(self.tokens) - 1:
            self.pos += 1
        return token

    def _match(self, token_type: _TokenType) -> bool:
        """Check if current token matches type, and advance if so."""
        if self._current().type == token_type:
            self._advance()
            return True
        return False

    def _expect(self, token_type: _TokenType) -> _Token:
        """Expect a specific token type, raise error if not found."""
        if self._current().type != token_type:
            raise ValueError(f"Expected {token_type.name}, got {self._current().type.name}")
        return self._advance()

    def _or_expr(self) -> bool:
        """Parse OR expression (lowest precedence)."""
        left = self._and_expr()
        while self._match(_TokenType.OR):
            right = self._and_expr()
            left = left or right
        return left

    def _and_expr(self) -> bool:
        """Parse AND expression (higher precedence than OR)."""
        left = self._unary_expr()
        while self._match(_TokenType.AND):
            right = self._unary_expr()
            left = left and right
        return left

    def _unary_expr(self) -> bool:
        """Parse unary NOT expression."""
        if self._match(_TokenType.NOT):
            return not self._unary_expr()
        return self._primary()

    def _primary(self) -> bool:
        """Parse primary expressions (comparisons, existence, hasPrefix, parens)."""
        # Parenthesized expression
        if self._match(_TokenType.LPAREN):
            result = self._or_expr()
            self._expect(_TokenType.RPAREN)
            return result

        # hasPrefix function
        if self._match(_TokenType.HAS_PREFIX):
            return self._parse_has_prefix()

        # Existence check: attributes:key
        if self._match(_TokenType.ATTRIBUTES_COLON):
            key = self._expect(_TokenType.IDENTIFIER).value
            return key in self.attributes

        # Comparison: attributes.key = "value" or attributes.key != "value"
        if self._match(_TokenType.ATTRIBUTES_DOT):
            return self._parse_comparison()

        raise ValueError(f"Unexpected token: {self._current().value}")

    def _parse_comparison(self) -> bool:
        """Parse a comparison expression (= or !=)."""
        key = self._expect(_TokenType.IDENTIFIER).value

        if self._match(_TokenType.EQUALS):
            value = self._expect(_TokenType.STRING).value
            return self.attributes.get(key) == value

        if self._match(_TokenType.NOT_EQUALS):
            value = self._expect(_TokenType.STRING).value
            return self.attributes.get(key) != value

        raise ValueError(f"Expected = or !=, got {self._current().value}")

    def _parse_has_prefix(self) -> bool:
        """Parse hasPrefix(attributes.key, "prefix") function."""
        self._expect(_TokenType.LPAREN)
        self._expect(_TokenType.ATTRIBUTES_DOT)
        key = self._expect(_TokenType.IDENTIFIER).value
        self._expect(_TokenType.COMMA)
        prefix = self._expect(_TokenType.STRING).value
        self._expect(_TokenType.RPAREN)

        attr_value = self.attributes.get(key)
        if attr_value is None:
            return False
        return attr_value.startswith(prefix)


def matches_filter(attributes: dict[str, str], filter_expression: str) -> bool:
    """Evaluate a Pub/Sub filter expression against message attributes.

    Supports common CEL subset used in Google Pub/Sub:
    - attributes.key = "value" (exact match)
    - attributes.key != "value" (not equal)
    - attributes:key (existence check)
    - hasPrefix(attributes.key, "prefix") (prefix match)
    - AND, OR, NOT logical operators
    - Parentheses for grouping

    Args:
        attributes: Message attributes dictionary.
        filter_expression: The filter expression string.

    Returns:
        True if the attributes match the filter, False otherwise.

    Raises:
        ValueError: If the filter expression is invalid.

    Example:
        >>> matches_filter({"type": "order"}, 'attributes.type = "order"')
        True
        >>> matches_filter({"type": "user"}, 'attributes.type = "order"')
        False
        >>> matches_filter({"priority": "high"}, "attributes:priority")
        True
    """
    if not filter_expression or not filter_expression.strip():
        return True  # No filter = match all

    tokens = _tokenize(filter_expression.strip())
    parser = _FilterParser(tokens, attributes)
    return parser.parse()


# =============================================================================
# PubSubTestClient
# =============================================================================


class PubSubTestClient:
    """A test wrapper for PubSubBroker that enables in-memory message routing.

    This allows testing subscriber handlers without needing a real PubSub emulator,
    making tests fast and isolated. Supports filter expression evaluation to match
    Google Pub/Sub behavior.

    Example:
        ```python
        broker = PubSubBroker(project_id="test")


        @broker.subscriber(
            alias="orders",
            topic_name="events",
            subscription_name="orders-sub",
            filter_expression='attributes.type = "order"',
        )
        async def handler(msg: Message) -> None:
            logger.info(f"Processed: {msg.data}")


        async with PubSubTestClient(broker) as test_client:
            # This message will be routed to handler
            await test_client.publish(
                "Hello",
                topic="events",
                attributes={"type": "order"},
            )

            # This message will NOT be routed (wrong type)
            await test_client.publish(
                "Ignored",
                topic="events",
                attributes={"type": "user"},
            )
        ```
    """

    def __init__(self, broker: PubSubBroker, **kwargs: Any) -> None:
        """Initialize test broker wrapper.

        Args:
            broker: The real PubSubBroker to wrap.
            **kwargs: Additional configuration (for future extensibility).
        """
        self.broker = broker
        self._patchers: list[Any] = []
        self._published_messages: list[PublishedMessage] = []
        self._processing_results: list[ProcessingResult] = []
        self._mock_client: MagicMock | None = None

    async def __aenter__(self) -> PubSubTestClient:
        """Enter async context manager."""
        await self._start_patches()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit async context manager."""
        self._stop_patches()

    async def _start_patches(self) -> None:
        """Start all mocking patches."""
        # Configure mock client
        self._mock_client = MagicMock()
        self._mock_client.create_topic = AsyncMock()
        self._mock_client.create_subscription = AsyncMock()
        self._mock_client.update_subscription = AsyncMock()
        self._mock_client.publish = AsyncMock(side_effect=self._fake_publish)

        # Mock PubSubClient at the source module and at every import site
        # so that all publish paths (including broker.publish() from within
        # handlers) are intercepted by _fake_publish.
        for target in (
            "fastpubsub.clients.pubsub.PubSubClient",
            "fastpubsub.middlewares.di.PubSubClient",
        ):
            patcher = patch(target)
            mock_class = patcher.start()
            mock_class.return_value = self._mock_client
            self._patchers.append(patcher)

        # Mock the builder to avoid real PubSub operations
        builder_patcher = patch("fastpubsub.builder.PubSubSubscriptionBuilder")
        mock_builder_class = builder_patcher.start()
        self._patchers.append(builder_patcher)

        mock_builder = MagicMock()
        mock_builder.build = AsyncMock()
        mock_builder_class.return_value = mock_builder

        # Mock the task manager to not actually start async tasks
        task_manager_patcher = patch.object(self.broker.task_manager, "start", AsyncMock())
        task_manager_patcher.start()
        self._patchers.append(task_manager_patcher)

        task_manager_shutdown_patcher = patch.object(
            self.broker.task_manager, "shutdown", AsyncMock()
        )
        task_manager_shutdown_patcher.start()
        self._patchers.append(task_manager_shutdown_patcher)

    def _stop_patches(self) -> None:
        """Stop all mocking patches."""
        for patcher in self._patchers:
            patcher.stop()
        self._patchers.clear()

    async def _fake_publish(
        self,
        topic_name: str,
        data: bytes,
        ordering_key: str | None = None,
        attributes: dict[str, str] | None = None,
        project_id: str | None = None,
    ) -> str:
        """Fake publish that routes messages to matching subscribers.

        Routes messages based on topic name, project_id, AND filter expression
        matching. If a subscriber has a filter expression, only messages matching
        that filter will be delivered to the subscriber.

        Args:
            topic_name: Target topic.
            data: Message data.
            ordering_key: Ordering key (unused in test).
            attributes: Message attributes.
            project_id: Target project ID. Defaults to broker's project_id.

        Returns:
            Fake message ID.
        """
        resolved_project_id = project_id or self.broker.project_id

        self._published_messages.append(
            PublishedMessage(
                topic_name=topic_name,
                data=data,
                attributes=attributes,
                project_id=resolved_project_id,
            )
        )

        subscribers = self.broker.router._get_subscribers()
        for subscriber in subscribers.values():
            if subscriber.topic_name != topic_name:
                continue

            subscriber_project_id = subscriber.project_id or self.broker.project_id
            if subscriber_project_id != resolved_project_id:
                continue

            filter_expr = subscriber.delivery_policy.filter_expression
            if not matches_filter(attributes or {}, filter_expr):
                continue

            message = Message(
                id=f"test-msg-{len(self._published_messages)}",
                data=data,
                size=len(data),
                attributes=attributes or {},
                delivery_attempt=1,
                project_id=resolved_project_id,
                topic_name=topic_name,
                subscriber_name=subscriber.name,
            )

            callstack = subscriber._build_callstack()
            return_value = None
            error = None
            try:
                return_value = await callstack.on_message(message)
            except BaseException as exc:
                error = exc

            self._processing_results.append(
                ProcessingResult(
                    message=message,
                    return_value=return_value,
                    error=error,
                )
            )

        return f"test-msg-{len(self._published_messages)}"

    async def publish(
        self,
        data: Any,
        topic: str,
        ordering_key: str | None = None,
        attributes: dict[str, str] | None = None,
        project_id: str = "",
    ) -> None:
        """Publish a message for testing.

        Args:
            data: Message data (will be encoded).
            topic: Topic name.
            ordering_key: Ordering key.
            attributes: Message attributes.
            project_id: Target project ID. Defaults to broker's project_id.
        """
        resolved_project_id = project_id or self.broker.project_id
        encoded_data = await Publisher._serialize_message(data)
        await self._fake_publish(
            topic, encoded_data, ordering_key, attributes, project_id=resolved_project_id
        )

    def get_published_messages(self) -> list[PublishedMessage]:
        """Get all published messages for inspection.

        Returns:
            A copy of the list of published messages.

        Example:
            ```python
            from fastpubsub import PubSubBroker, Message
            from fastpubsub.testing import PubSubTestClient

            broker = PubSubBroker(project_id="my-project")


            @broker.subscriber(
                alias="orders",
                topic_name="order-events",
                subscription_name="orders-sub",
            )
            async def process_order(msg: Message) -> None: ...


            async with PubSubTestClient(broker) as client:
                await client.publish({"id": 1}, topic="order-events", attributes={"region": "us"})
                await client.publish({"id": 2}, topic="order-events", project_id="other-project")

                messages = client.get_published_messages()
                assert len(messages) == 2
                assert messages[0].topic_name == "order-events"
                assert messages[0].data == b'{"id": 1}'
                assert messages[0].attributes == {"region": "us"}
                assert messages[0].project_id == "my-project"

                assert messages[1].topic_name == "order-events"
                assert messages[1].data == b'{"id": 2}'
                assert messages[1].attributes == {}
                assert messages[1].project_id == "other-project"
            ```
        """
        return self._published_messages.copy()

    def get_results(self) -> list[ProcessingResult]:
        """Get all processing results recorded during this session.

        Each result holds the ``message`` delivered to the subscriber,
        the handler's ``return_value``, and any ``error`` that was raised.
        Metadata like subscriber name, topic, and project are available
        through ``result.message``.

        Returns:
            A copy of the list of processing results.

        Example:
            ```python
            from fastpubsub import PubSubBroker, Message
            from fastpubsub.testing import PubSubTestClient

            broker = PubSubBroker(project_id="my-project")


            @broker.subscriber(
                alias="payments",
                topic_name="payment-events",
                subscription_name="payments-sub",
            )
            async def process_payment(msg: Message) -> str:
                return "accepted"


            @broker.subscriber(
                alias="analytics",
                topic_name="payment-events",
                subscription_name="analytics-sub",
                project_id="analytics-project",
            )
            async def track_payment(msg: Message) -> None:
                raise ValueError("tracking failed")


            async with PubSubTestClient(broker) as client:
                # Publish to the default project: Only process_payment runs
                await client.publish({"amount": 100}, topic="payment-events")

                # Publish to the analytics project: Only track_payment runs
                await client.publish(
                    {"amount": 100},
                    topic="payment-events",
                    project_id="analytics-project",
                )

                results = client.get_results()
                assert len(results) == 2

                assert results[0].message.subscriber_name == "process_payment"
                assert results[0].return_value == "accepted"
                assert results[0].error is None

                assert results[1].message.subscriber_name == "track_payment"
                assert results[1].return_value is None
                assert isinstance(errors[1].error, ValueError)
            ```
        """
        return self._processing_results.copy()

    def clear_published_messages(self) -> None:
        """Clear all published messages."""
        self._published_messages.clear()

    def clear_results(self) -> None:
        """Clear all results."""
        self._processing_results.clear()
