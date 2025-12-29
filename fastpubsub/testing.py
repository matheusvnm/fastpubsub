"""Testing utilities for FastPubSub."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from fastpubsub.datastructures import Message

if TYPE_CHECKING:
    from fastpubsub.broker import PubSubBroker

__all__ = ["PubSubTestClient"]


class PubSubTestClient:
    """A test wrapper for PubSubBroker that enables in-memory message routing.

    This allows testing subscriber handlers without needing a real PubSub emulator,
    making tests fast and isolated.

    Example:
        ```python
        broker = PubSubBroker(project_id="test")


        @broker.subscriber(alias="test", topic_name="test-topic", subscription_name="test-sub")
        async def handler(msg: str) -> str:
            return f"Processed: {msg}"


        async with TestPubSubBroker(broker) as test_broker:
            await test_broker.publish("Hello", topic="test-topic")
        ```
    """

    def __init__(self, broker: PubSubBroker, **kwargs: Any) -> None:
        """Initialize test broker wrapper.

        Args:
            broker: The real PubSubBroker to wrap
            **kwargs: Additional configuration (for future extensibility)
        """
        self.broker = broker
        self._patchers: list[Any] = []
        self._published_messages: list[tuple[str, bytes, dict[str, str] | None]] = []
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
        # Mock PubSubClient to prevent real PubSub calls
        client_patcher = patch("fastpubsub.clients.pubsub.PubSubClient")
        mock_client_class = client_patcher.start()
        self._patchers.append(client_patcher)

        # Configure mock client
        self._mock_client = MagicMock()
        self._mock_client.create_topic = AsyncMock()
        self._mock_client.create_subscription = AsyncMock()
        self._mock_client.update_subscription = AsyncMock()
        self._mock_client.publish = AsyncMock(side_effect=self._fake_publish)
        mock_client_class.return_value = self._mock_client

        # Mock the builder to avoid real PubSub operations
        builder_patcher = patch("fastpubsub.builder.PubSubSubscriptionBuilder")
        mock_builder_class = builder_patcher.start()
        self._patchers.append(builder_patcher)

        mock_builder = MagicMock()
        mock_builder.build = AsyncMock()
        mock_builder_class.return_value = mock_builder

        # Mock the task manager to not actually start async tasks
        task_manager_patcher = patch.object(self.broker.task_manager, "start", MagicMock())
        task_manager_patcher.start()
        self._patchers.append(task_manager_patcher)

        task_manager_shutdown_patcher = patch.object(
            self.broker.task_manager, "shutdown", MagicMock()
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
    ) -> str:
        """Fake publish that routes messages to matching subscribers.

        Args:
            topic_name: Target topic
            data: Message data
            ordering_key: Ordering key (unused in test)
            attributes: Message attributes

        Returns:
            Fake message ID
        """
        # Store for inspection
        self._published_messages.append((topic_name, data, attributes))

        # Find matching subscribers from the router
        subscribers = self.broker.router._get_subscribers()
        for subscriber in subscribers.values():
            if subscriber.topic_name == topic_name:
                # Create message
                message = Message(
                    id=f"test-msg-{len(self._published_messages)}",
                    size=len(data),
                    data=data,
                    attributes=attributes or {},
                    delivery_attempt=1,
                    project_id=self.broker.project_id,
                    topic_name=topic_name,
                    subscriber_name=subscriber.name,
                )

                # Build callstack and invoke handler
                callstack = subscriber._build_callstack()
                await callstack.on_message(message)

        return f"test-msg-{len(self._published_messages)}"

    async def publish(
        self,
        data: Any,
        topic: str,
        ordering_key: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Publish a message for testing.

        Args:
            data: Message data (will be encoded)
            topic: Topic name
            ordering_key: Ordering key
            attributes: Message attributes
        """
        # Encode data (mimic real publish)
        if isinstance(data, str):
            encoded_data = data.encode()
        elif isinstance(data, bytes):
            encoded_data = data
        else:
            encoded_data = json.dumps(data).encode()

        await self._fake_publish(topic, encoded_data, ordering_key, attributes)

    def get_published_messages(
        self,
    ) -> list[tuple[str, bytes, dict[str, str] | None]]:
        """Get all published messages for inspection."""
        return self._published_messages.copy()

    def clear_published_messages(self) -> None:
        """Clear all published messages."""
        self._published_messages.clear()
