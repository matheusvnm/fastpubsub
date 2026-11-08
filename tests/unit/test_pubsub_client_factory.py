"""Unit tests for PubSubClientFactory."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from fastpubsub.clients.factory import PubSubClientFactory


@pytest.fixture(autouse=True)
def reset_factory():
    """Reset factory state before each test."""
    yield
    PubSubClientFactory.clear_cache()


class TestPubSubClientFactory:
    """Test the PubSubClientFactory singleton caching."""

    @pytest.mark.asyncio
    async def test_get_publisher_caches_by_project_and_ordering(self):
        """Test that publishers are cached by (project_id, enable_ordering)."""
        with patch(
            "fastpubsub.clients.factory.PublisherClient"
        ) as mock_publisher_class:
            mock_publisher = MagicMock()
            mock_publisher_class.return_value = mock_publisher

            client1 = await PubSubClientFactory.get_publisher(
                "test-project", enable_ordering=False
            )
            assert client1 == mock_publisher
            assert mock_publisher_class.call_count == 1

            client2 = await PubSubClientFactory.get_publisher(
                "test-project", enable_ordering=False
            )
            assert client2 == mock_publisher
            assert mock_publisher_class.call_count == 1

    @pytest.mark.asyncio
    async def test_get_publisher_different_ordering_creates_separate_clients(
        self,
    ):
        """Test that different ordering flags create separate clients."""
        with patch(
            "fastpubsub.clients.factory.PublisherClient"
        ) as mock_publisher_class:
            mock_publisher1 = MagicMock()
            mock_publisher2 = MagicMock()
            mock_publisher_class.side_effect = [
                mock_publisher1,
                mock_publisher2,
            ]

            client1 = await PubSubClientFactory.get_publisher(
                "test-project", enable_ordering=False
            )
            assert client1 == mock_publisher1

            client2 = await PubSubClientFactory.get_publisher(
                "test-project", enable_ordering=True
            )
            assert client2 == mock_publisher2

            assert mock_publisher_class.call_count == 2
            assert client1 != client2

    @pytest.mark.asyncio
    async def test_get_subscriber_caches_by_project(self):
        """Test that subscribers are cached by project_id."""
        with patch(
            "fastpubsub.clients.factory.SubscriberClient"
        ) as mock_subscriber_class:
            mock_subscriber = MagicMock()
            mock_subscriber_class.return_value = mock_subscriber

            client1 = await PubSubClientFactory.get_subscriber("test-project")
            assert client1 == mock_subscriber
            assert mock_subscriber_class.call_count == 1

            client2 = await PubSubClientFactory.get_subscriber("test-project")
            assert client2 == mock_subscriber
            assert mock_subscriber_class.call_count == 1

    @pytest.mark.asyncio
    async def test_get_subscriber_same_project_returns_cached(self):
        """Test that same project_id returns cached subscriber."""
        with patch(
            "fastpubsub.clients.factory.SubscriberClient"
        ) as mock_subscriber_class:
            mock_subscriber = MagicMock()
            mock_subscriber_class.return_value = mock_subscriber

            client1 = await PubSubClientFactory.get_subscriber("project-a")
            client2 = await PubSubClientFactory.get_subscriber("project-a")
            client3 = await PubSubClientFactory.get_subscriber("project-a")

            assert client1 is client2
            assert client2 is client3

            assert mock_subscriber_class.call_count == 1

    @pytest.mark.asyncio
    async def test_concurrent_access_creates_single_client(self):
        with patch(
            "fastpubsub.clients.factory.PublisherClient"
        ) as mock_publisher_class:
            mock_publisher = MagicMock()
            mock_publisher_class.return_value = mock_publisher

            clients = await asyncio.gather(
                PubSubClientFactory.get_publisher(
                    "test-project", enable_ordering=False
                ),
                PubSubClientFactory.get_publisher(
                    "test-project", enable_ordering=False
                ),
                PubSubClientFactory.get_publisher(
                    "test-project", enable_ordering=False
                ),
                PubSubClientFactory.get_publisher(
                    "test-project", enable_ordering=False
                ),
            )

            assert all(client == mock_publisher for client in clients)
            assert mock_publisher_class.call_count == 1

    @pytest.mark.asyncio
    async def test_close_all_closes_all_cached_clients(self):
        """Test that close_all closes all publishers and subscribers."""
        with (
            patch(
                "fastpubsub.clients.factory.PublisherClient"
            ) as mock_publisher_class,
            patch(
                "fastpubsub.clients.factory.SubscriberClient"
            ) as mock_subscriber_class,
        ):
            mock_publisher = MagicMock()
            mock_publisher.transport.close = MagicMock()
            mock_publisher_class.return_value = mock_publisher

            mock_subscriber = MagicMock()
            mock_subscriber.transport.close = MagicMock()
            mock_subscriber_class.return_value = mock_subscriber

            await PubSubClientFactory.get_publisher(
                "project-1", enable_ordering=False
            )
            await PubSubClientFactory.get_publisher(
                "project-1", enable_ordering=True
            )
            await PubSubClientFactory.get_subscriber("project-1")

            await PubSubClientFactory.close_all()

            assert mock_publisher.transport.close.call_count == 2
            assert mock_subscriber.transport.close.call_count == 1

    @pytest.mark.asyncio
    async def test_close_all_clears_cache(self):
        """Test that close_all clears the cache after closing."""
        with (
            patch(
                "fastpubsub.clients.factory.PublisherClient"
            ) as mock_publisher_class,
            patch(
                "fastpubsub.clients.factory.SubscriberClient"
            ) as mock_subscriber_class,
        ):
            mock_publisher = MagicMock()
            mock_publisher.transport.close = MagicMock()
            mock_publisher_class.return_value = mock_publisher

            mock_subscriber = MagicMock()
            mock_subscriber.transport.close = MagicMock()
            mock_subscriber_class.return_value = mock_subscriber

            await PubSubClientFactory.get_publisher(
                "project-1", enable_ordering=False
            )
            await PubSubClientFactory.get_subscriber("project-1")

            assert len(PubSubClientFactory._publisher_cache) == 1
            assert len(PubSubClientFactory._subscriber_cache) == 1

            await PubSubClientFactory.close_all()

            assert len(PubSubClientFactory._publisher_cache) == 0
            assert len(PubSubClientFactory._subscriber_cache) == 0

    @pytest.mark.asyncio
    async def test_close_all_handles_exceptions_gracefully(self):
        """Test that close_all continues on client close errors."""
        with (
            patch(
                "fastpubsub.clients.factory.PublisherClient"
            ) as mock_publisher_class,
            patch(
                "fastpubsub.clients.factory.SubscriberClient"
            ) as mock_subscriber_class,
        ):
            mock_publisher = MagicMock()
            mock_publisher.transport.close.side_effect = Exception(
                "Transport error"
            )
            mock_publisher_class.return_value = mock_publisher

            mock_subscriber = MagicMock()
            mock_subscriber.transport.close = MagicMock()
            mock_subscriber_class.return_value = mock_subscriber

            await PubSubClientFactory.get_publisher(
                "project-1", enable_ordering=False
            )
            await PubSubClientFactory.get_subscriber("project-1")

            await PubSubClientFactory.close_all()

            assert len(PubSubClientFactory._publisher_cache) == 0
            assert len(PubSubClientFactory._subscriber_cache) == 0

    def test_clear_cache_without_closing(self):
        """Test that clear_cache clears without closing connections."""
        with patch(
            "fastpubsub.clients.factory.PublisherClient"
        ) as mock_publisher_class:
            mock_publisher = MagicMock()
            mock_publisher.transport.close = MagicMock()
            mock_publisher_class.return_value = mock_publisher

            PubSubClientFactory._publisher_cache[("project-1", False)] = (
                mock_publisher
            )

            PubSubClientFactory.clear_cache()

            assert len(PubSubClientFactory._publisher_cache) == 0
            mock_publisher.transport.close.assert_not_called()
