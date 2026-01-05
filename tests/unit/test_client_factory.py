"""Tests for PubSubClientFactory."""

from __future__ import annotations

import asyncio
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from fastpubsub.clients.factory import PubSubClientFactory

FACTORY_MODULE_PATH = "fastpubsub.clients.factory"


class TestPubSubClientFactory:
    @pytest.fixture(autouse=True)
    def clear_factory_cache(self) -> Generator[None]:
        """Clear the factory cache before and after each test."""
        PubSubClientFactory.clear_cache()
        yield
        PubSubClientFactory.clear_cache()

    @pytest.fixture
    def publisher_client(self) -> Generator[MagicMock]:
        with patch(f"{FACTORY_MODULE_PATH}.PublisherClient") as mock:
            yield mock

    @pytest.fixture
    def subscriber_client(self) -> Generator[MagicMock]:
        with patch(f"{FACTORY_MODULE_PATH}.SubscriberClient") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_get_publisher_creates_new_client(self, publisher_client: MagicMock):
        """Test that get_publisher creates a new client when cache is empty."""
        client = await PubSubClientFactory.get_publisher("test-project")

        assert client == publisher_client.return_value
        publisher_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_publisher_returns_cached_client(self, publisher_client: MagicMock):
        """Test that get_publisher returns cached client on subsequent calls."""
        client1 = await PubSubClientFactory.get_publisher("test-project")
        client2 = await PubSubClientFactory.get_publisher("test-project")

        assert client1 is client2
        publisher_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_publisher_caches_by_project_id(self, publisher_client: MagicMock):
        """Test that different project IDs create different clients."""
        client1 = await PubSubClientFactory.get_publisher("project-a")
        client2 = await PubSubClientFactory.get_publisher("project-b")

        assert client1 is client2  # Both use same mock return value
        assert publisher_client.call_count == 2

    @pytest.mark.asyncio
    async def test_get_publisher_caches_by_ordering(self, publisher_client: MagicMock):
        """Test that different ordering settings create different clients."""
        # Use side_effect to return different mock instances for each call
        mock_clients = [
            MagicMock(name="client_no_ordering"),
            MagicMock(name="client_with_ordering"),
        ]
        publisher_client.side_effect = mock_clients

        client1 = await PubSubClientFactory.get_publisher("test-project", enable_ordering=False)
        client2 = await PubSubClientFactory.get_publisher("test-project", enable_ordering=True)
        client3 = await PubSubClientFactory.get_publisher("test-project", enable_ordering=False)

        # client1 and client3 should be the same (cached), client2 should be different
        assert client1 is client3
        assert client1 is not client2
        assert publisher_client.call_count == 2

    @pytest.mark.asyncio
    async def test_get_publisher_uses_publisher_options(self, publisher_client: MagicMock):
        """Test that publisher options are correctly passed."""
        await PubSubClientFactory.get_publisher("test-project", enable_ordering=True)

        call_kwargs = publisher_client.call_args.kwargs
        assert "publisher_options" in call_kwargs
        assert call_kwargs["publisher_options"].enable_message_ordering is True

    @pytest.mark.asyncio
    async def test_get_subscriber_creates_new_client(self, subscriber_client: MagicMock):
        """Test that get_subscriber creates a new client when cache is empty."""
        client = await PubSubClientFactory.get_subscriber("test-project")

        assert client == subscriber_client.return_value
        subscriber_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_subscriber_returns_cached_client(self, subscriber_client: MagicMock):
        """Test that get_subscriber returns cached client on subsequent calls."""
        client1 = await PubSubClientFactory.get_subscriber("test-project")
        client2 = await PubSubClientFactory.get_subscriber("test-project")

        assert client1 is client2
        subscriber_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_subscriber_caches_by_project_id(self, subscriber_client: MagicMock):
        """Test that different project IDs create different clients."""
        await PubSubClientFactory.get_subscriber("project-a")
        await PubSubClientFactory.get_subscriber("project-b")

        assert subscriber_client.call_count == 2

    @pytest.mark.asyncio
    async def test_close_all_closes_publishers(
        self, publisher_client: MagicMock, subscriber_client: MagicMock
    ):
        """Test that close_all closes all cached publisher clients."""
        await PubSubClientFactory.get_publisher("project-a")
        await PubSubClientFactory.get_publisher("project-b", enable_ordering=True)

        await PubSubClientFactory.close_all()

        # Transport.close should be called for each publisher
        assert publisher_client.return_value.transport.close.call_count == 2

    @pytest.mark.asyncio
    async def test_close_all_closes_subscribers(
        self, publisher_client: MagicMock, subscriber_client: MagicMock
    ):
        """Test that close_all closes all cached subscriber clients."""
        await PubSubClientFactory.get_subscriber("project-a")
        await PubSubClientFactory.get_subscriber("project-b")

        await PubSubClientFactory.close_all()

        # Transport.close should be called for each subscriber
        assert subscriber_client.return_value.transport.close.call_count == 2

    @pytest.mark.asyncio
    async def test_close_all_clears_cache(
        self, publisher_client: MagicMock, subscriber_client: MagicMock
    ):
        """Test that close_all clears the cache."""
        await PubSubClientFactory.get_publisher("test-project")
        await PubSubClientFactory.get_subscriber("test-project")

        await PubSubClientFactory.close_all()

        # Cache should be empty now
        assert len(PubSubClientFactory._publisher_cache) == 0
        assert len(PubSubClientFactory._subscriber_cache) == 0

    @pytest.mark.asyncio
    async def test_close_all_handles_errors(
        self, publisher_client: MagicMock, subscriber_client: MagicMock
    ):
        """Test that close_all handles errors gracefully."""
        publisher_client.return_value.transport.close.side_effect = Exception("Close failed")

        await PubSubClientFactory.get_publisher("test-project")

        # Should not raise, just log the error
        await PubSubClientFactory.close_all()

        # Cache should still be cleared
        assert len(PubSubClientFactory._publisher_cache) == 0

    def test_clear_cache_resets_state(self):
        """Test that clear_cache resets all internal state."""
        # Add some fake data to the cache
        PubSubClientFactory._publisher_cache[("test", False)] = MagicMock()
        PubSubClientFactory._subscriber_cache["test"] = MagicMock()
        PubSubClientFactory._lock = asyncio.Lock()

        PubSubClientFactory.clear_cache()

        assert len(PubSubClientFactory._publisher_cache) == 0
        assert len(PubSubClientFactory._subscriber_cache) == 0
        assert PubSubClientFactory._lock is None

    @pytest.mark.asyncio
    async def test_concurrent_access_creates_single_client(self, publisher_client: MagicMock):
        """Test that concurrent access creates only one client."""

        async def get_client():
            return await PubSubClientFactory.get_publisher("test-project")

        # Run multiple concurrent requests
        results = await asyncio.gather(*[get_client() for _ in range(10)])

        # All should return the same client
        assert all(r is results[0] for r in results)
        # Only one client should have been created
        publisher_client.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_lock_creates_lock_lazily(self):
        """Test that the lock is created lazily."""
        # Clear to ensure no lock exists
        PubSubClientFactory._lock = None

        lock = PubSubClientFactory._get_lock()

        assert lock is not None
        assert isinstance(lock, asyncio.Lock)

    @pytest.mark.asyncio
    async def test_get_lock_returns_same_lock(self):
        """Test that get_lock returns the same lock instance."""
        PubSubClientFactory._lock = None

        lock1 = PubSubClientFactory._get_lock()
        lock2 = PubSubClientFactory._get_lock()

        assert lock1 is lock2
