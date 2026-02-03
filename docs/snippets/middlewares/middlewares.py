"""Title: Custom Middleware Implementations (Helper Module)

Provides custom middleware classes used by other middleware examples.

This module defines several middleware classes demonstrating different scopes:
- BrokerMiddleware: Runs on all broker handlers and their children
- RouterMiddleware: Runs on router handlers and their children
- SubRouterMiddleware: Runs on sub-router handlers
- SubcriberMiddleware: Runs only at subscriber level
- PublisherMiddleware: Runs only on publish operations

Each middleware implements both on_message (for receiving) and on_publish
(for sending) methods to show the complete middleware interface.

This is a helper module imported by other examples, not meant to be run directly.
"""

from typing import Any

from fastpubsub import BaseMiddleware, Message
from fastpubsub.logger import logger


class BrokerMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the broker subscriber middleware! "
            "I will only be executed at broker handlers and its children"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the broker publish middleware! "
            "I will only be executed at broker publish and its children"
        )
        return await super().on_publish(data, ordering_key, attributes)


class RouterMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the router subscriber middleware! "
            "I will only be executed at the router handlers and its children"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the router publish middleware! "
            "I will only be executed at the router publish and its children"
        )
        return await super().on_publish(data, ordering_key, attributes)


class SubRouterMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the sub-router subscriber middleware! "
            "I will only be executed at the sub-child router handlers"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the sub-router publish middleware! "
            "I will only be executed at the sub-child router publishers"
        )
        return await super().on_publish(data, ordering_key, attributes)


class SubcriberMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info("I'm the subscriber middleware! I will only be executed at subscriber level")
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        pass


class PublisherMiddleware(BaseMiddleware):
    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info("I'm the publisher middleware! I will only be executed at publisher level")
        return await super().on_publish(data, ordering_key, attributes)

    async def on_message(self, message: Message) -> Any:
        pass
