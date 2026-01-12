from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastpubsub.concurrency.manager import AsyncTaskManager

PUBSUB_POLL_TASK_MODULE_PATH = "fastpubsub.concurrency.tasks"
ASYNC_TASK_MANAGER_MODULE_PATH = "fastpubsub.concurrency.manager"


class TestAsyncTaskManager:
    @pytest.fixture()
    def task(self) -> Generator[MagicMock]:
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubStreamingPullTask") as streaming_task:
            streaming_task.return_value.start = AsyncMock()
            streaming_task.return_value.shutdown = AsyncMock()
            yield streaming_task

    def test_create_task(self, task: MagicMock):
        mock_subscriber = MagicMock()

        task_manager = AsyncTaskManager()
        task_manager.create_task(mock_subscriber)

        created_task = task_manager._tasks.pop(0)

        assert not task_manager._tasks
        assert created_task == task.return_value
        assert task.call_args[0][0] == mock_subscriber

    def test_alive_check(self, task: MagicMock):
        mock_subscriber = MagicMock()
        mock_subscriber.name = "sub_name"
        task.return_value.subscriber = mock_subscriber
        task.return_value.task_alive.return_value = True

        task_manager = AsyncTaskManager()
        task_manager.create_task(mock_subscriber)
        liveness = task_manager.alive()

        assert isinstance(liveness, dict)
        assert len(liveness) == 1
        assert mock_subscriber.name in liveness
        assert liveness[mock_subscriber.name]

    @pytest.mark.asyncio
    async def test_ready_check(self, task: MagicMock):
        mock_subscriber = MagicMock()
        mock_subscriber.ready = True
        mock_subscriber.name = "sub_name"
        task.return_value.subscriber = mock_subscriber
        task.return_value.task_ready.return_value = True

        task_manager = AsyncTaskManager()
        task_manager.create_task(mock_subscriber)
        await task_manager.start()

        readiness = task_manager.ready()

        assert isinstance(readiness, dict)
        assert len(readiness) == 1
        assert mock_subscriber.name in readiness
        assert readiness[mock_subscriber.name]

    @pytest.mark.asyncio
    async def test_start(self, task: MagicMock):
        task_manager = AsyncTaskManager()
        task_manager.create_task(MagicMock())
        await task_manager.start()

        task.assert_called_once()
        task.return_value.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown(self, task: MagicMock):
        task_manager = AsyncTaskManager()
        task_manager.create_task(MagicMock())
        await task_manager.start()
        await task_manager.shutdown()

        task.assert_called_once()
        task.return_value.start.assert_called_once()
        task.return_value.shutdown.assert_called_once()


class TestAsyncTaskManagerShutdown:
    """Test graceful shutdown functionality."""

    @pytest.fixture()
    def task(self) -> Generator[MagicMock]:
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubStreamingPullTask") as streaming_task:
            streaming_task.return_value.start = AsyncMock()
            streaming_task.return_value.shutdown = AsyncMock()
            streaming_task.return_value.task_alive = MagicMock(return_value=True)
            yield streaming_task

    @pytest.mark.asyncio
    async def test_shutdown_calls_task_shutdown_with_timeout(self, task: MagicMock):
        """Test that shutdown calls each task's shutdown with timeout."""
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubClientFactory") as mock_factory:
            mock_factory.close_all = AsyncMock()

            task_manager = AsyncTaskManager()
            task_manager.create_task(MagicMock())
            task_manager.create_task(MagicMock())
            await task_manager.start()

            await task_manager.shutdown(timeout=45.0)

            assert task.return_value.shutdown.call_count == 2
            task.return_value.shutdown.assert_called_with(timeout=45.0)

    @pytest.mark.asyncio
    async def test_shutdown_clears_tasks_list(self, task: MagicMock):
        """Test that shutdown clears the tasks list."""
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubClientFactory") as mock_factory:
            mock_factory.close_all = AsyncMock()

            task_manager = AsyncTaskManager()
            task_manager.create_task(MagicMock())
            task_manager.create_task(MagicMock())

            # Verify tasks were added
            assert len(task_manager._tasks) == 2

            await task_manager.shutdown()

            # Verify tasks list is cleared
            assert len(task_manager._tasks) == 0

    @pytest.mark.asyncio
    async def test_shutdown_closes_factory_clients(self, task: MagicMock):
        """Test that shutdown calls PubSubClientFactory.close_all()."""
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubClientFactory") as mock_factory:
            mock_factory.close_all = AsyncMock()

            task_manager = AsyncTaskManager()
            task_manager.create_task(MagicMock())

            await task_manager.shutdown()

            # Verify factory close_all was called
            mock_factory.close_all.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_skips_dead_tasks(self, task: MagicMock):
        """Test that shutdown only processes alive tasks."""
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubClientFactory") as mock_factory:
            mock_factory.close_all = AsyncMock()

            # Set task_alive to return False
            task.return_value.task_alive.return_value = False

            task_manager = AsyncTaskManager()
            task_manager.create_task(MagicMock())

            await task_manager.shutdown()

            # Verify shutdown was not called on dead task
            task.return_value.shutdown.assert_not_called()

    @pytest.mark.asyncio
    async def test_shutdown_propagates_timeout_to_tasks(self, task: MagicMock):
        """Test that custom timeout is propagated."""
        with patch(f"{ASYNC_TASK_MANAGER_MODULE_PATH}.PubSubClientFactory") as mock_factory:
            mock_factory.close_all = AsyncMock()

            task_manager = AsyncTaskManager()
            task_manager.create_task(MagicMock())

            await task_manager.shutdown(timeout=60.0)
            task.return_value.shutdown.assert_called_once_with(timeout=60.0)
