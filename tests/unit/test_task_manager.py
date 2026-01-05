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
        task.return_value.start = AsyncMock()

        task_manager = AsyncTaskManager()
        task_manager.create_task(mock_subscriber)
        await task_manager.start()

        readiness = task_manager.ready()

        assert isinstance(readiness, dict)
        assert len(readiness) == 1
        assert mock_subscriber.name in readiness
        assert readiness[mock_subscriber.name]

    @pytest.mark.asyncio
    async def test_start_shutdown(self, task: MagicMock):
        task_instance = task.return_value
        task_instance.start = AsyncMock()
        task_instance.shutdown = AsyncMock()

        task_manager = AsyncTaskManager()
        task_manager.create_task(MagicMock())
        await task_manager.start()
        task_instance.start.assert_called_once()

        await task_manager.shutdown()
        task_instance.shutdown.assert_called_once()
