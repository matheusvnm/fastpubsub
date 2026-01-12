"""Thread-safe factory for Pub/Sub clients with async-friendly singleton caching.

This module provides a centralized factory for managing Google Cloud Pub/Sub
client instances. It ensures efficient reuse of gRPC connections by caching
clients based on project_id and configuration options.

Key features:
- Async-friendly locking using asyncio.Lock
- Caches PublisherClient by (project_id, enable_ordering)
- Caches SubscriberClient by project_id
- Provides graceful shutdown via close_all()
"""

from __future__ import annotations

import asyncio
import logging
from typing import ClassVar

from google.cloud.pubsub import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import PublisherOptions

logger = logging.getLogger(__name__)


class PubSubClientFactory:
    """Async-friendly factory for Pub/Sub clients with singleton caching.

    This factory ensures that only one client instance is created per unique
    combination of project_id and configuration. This follows Google's
    recommendation to reuse Pub/Sub clients for better performance.

    The caching strategy:
    - PublisherClient: Cached by (project_id, enable_ordering)
    - SubscriberClient: Cached by project_id

    Thread/async safety:
    - Uses asyncio.Lock for async-safe double-checked locking
    - Safe for concurrent access from multiple coroutines
    """

    _publisher_cache: ClassVar[dict[tuple[str, bool], PublisherClient]] = {}
    _subscriber_cache: ClassVar[dict[str, SubscriberClient]] = {}
    _lock: ClassVar[asyncio.Lock | None] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """Get or create the asyncio lock.

        Note: This creates the lock lazily to avoid issues with
        creating locks outside of an event loop.

        Returns:
            The shared asyncio.Lock instance.
        """
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

    @classmethod
    async def get_publisher(
        cls,
        project_id: str,
        enable_ordering: bool = False,
    ) -> PublisherClient:
        """Get or create a cached PublisherClient.

        Args:
            project_id: The Google Cloud project ID.
            enable_ordering: Whether to enable message ordering.

        Returns:
            A cached or newly created PublisherClient instance.
        """
        key = (project_id, enable_ordering)

        # Fast path: return cached client without acquiring lock
        if key in cls._publisher_cache:
            return cls._publisher_cache[key]

        # Slow path: acquire lock and create client if needed
        async with cls._get_lock():
            # Double-check after acquiring lock
            if key not in cls._publisher_cache:
                logger.debug(
                    f"Creating new PublisherClient for project={project_id}, "
                    f"ordering={enable_ordering}"
                )
                options = PublisherOptions(enable_message_ordering=enable_ordering)
                cls._publisher_cache[key] = PublisherClient(publisher_options=options)

        return cls._publisher_cache[key]

    @classmethod
    async def get_subscriber(cls, project_id: str) -> SubscriberClient:
        """Get or create a cached SubscriberClient.

        Args:
            project_id: The Google Cloud project ID.

        Returns:
            A cached or newly created SubscriberClient instance.
        """
        # Fast path: return cached client without acquiring lock
        if project_id in cls._subscriber_cache:
            return cls._subscriber_cache[project_id]

        # Slow path: acquire lock and create client if needed
        async with cls._get_lock():
            # Double-check after acquiring lock
            if project_id not in cls._subscriber_cache:
                logger.debug(f"Creating new SubscriberClient for project={project_id}")
                cls._subscriber_cache[project_id] = SubscriberClient()

        return cls._subscriber_cache[project_id]

    @classmethod
    async def close_all(cls) -> None:
        """Close all cached clients.

        This method should be called during application shutdown to ensure
        all gRPC connections are properly closed.
        """
        async with cls._get_lock():
            logger.debug("Closing all cached Pub/Sub clients")

            for key, client in cls._publisher_cache.items():
                try:
                    client.transport.close()
                    logger.debug(f"Closed PublisherClient for {key}")
                except Exception:
                    logger.exception(f"Error closing PublisherClient for {key}")

            for project_id, client in cls._subscriber_cache.items():
                try:
                    client.transport.close()
                    logger.debug(f"Closed SubscriberClient for {project_id}")
                except Exception:
                    logger.exception(f"Error closing SubscriberClient for {project_id}")

            cls._publisher_cache.clear()
            cls._subscriber_cache.clear()
            logger.debug("All Pub/Sub clients closed and cache cleared")

    @classmethod
    def clear_cache(cls) -> None:
        """Clear the client cache without closing connections.

        This is primarily useful for testing purposes.
        """
        cls._publisher_cache.clear()
        cls._subscriber_cache.clear()
        cls._lock = None
