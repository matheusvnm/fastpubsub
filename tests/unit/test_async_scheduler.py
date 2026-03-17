"""Unit tests for AsyncScheduler enhancements."""

import asyncio
from unittest.mock import MagicMock

import pytest
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from fastpubsub.clients.scheduler import AsyncScheduler


@pytest.fixture
async def scheduler() -> AsyncScheduler:
    """Create AsyncScheduler with running event loop."""
    loop = asyncio.get_running_loop()
    return AsyncScheduler(loop)


@pytest.fixture
def mock_message() -> MagicMock:
    """Create mock PubSubMessage."""
    msg = MagicMock(spec=PubSubMessage)
    msg.message_id = "test-123"
    msg.ack = MagicMock()
    msg.nack = MagicMock()
    msg.data = b"test data"
    msg.attributes = {}
    return msg


class TestAsyncSchedulerTracking:
    """Test message tracking functionality."""

    @pytest.mark.asyncio
    async def test_register_task_execution_adds_to_tracking(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that register_task_execution tracks tasks."""

        async def dummy_coro():
            await asyncio.sleep(0.1)

        task = asyncio.create_task(dummy_coro())

        scheduler.register_task_execution(task, mock_message)

        # Verify task is tracked
        assert id(task) in scheduler._executing_tasks
        assert scheduler._executing_tasks[id(task)] == mock_message

        await task

    @pytest.mark.asyncio
    async def test_deregister_executed_task_removes_from_tracking(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        async def dummy_coro():
            await asyncio.sleep(0.01)

        task = asyncio.create_task(dummy_coro())

        scheduler.register_task_execution(task, mock_message)
        assert id(task) in scheduler._executing_tasks

        await task
        await asyncio.sleep(0.01)
        assert id(task) not in scheduler._executing_tasks

    @pytest.mark.asyncio
    async def test_get_in_flight_count_returns_pending_and_executing(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        def dummy_callback(_):
            pass

        scheduler.schedule(dummy_callback, mock_message)

        # Create and register an executing task
        async def dummy_coro():
            await asyncio.sleep(0.5)

        task = asyncio.create_task(dummy_coro())
        scheduler.register_task_execution(task, mock_message)

        pending, executing = scheduler.get_in_flight_count()

        # Should have 1 pending (scheduled callback) and 1 executing (task)
        assert pending >= 1  # At least 1 pending
        assert executing == 1

        task.cancel()

    @pytest.mark.asyncio
    async def test_schedule_when_closed_nacks_message(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that scheduling when closed nacks the message."""
        scheduler.closed = True

        def dummy_callback(msg):
            pass

        scheduler.schedule(dummy_callback, mock_message)

        mock_message.nack.assert_called_once()

    @pytest.mark.asyncio
    async def test_wait_for_completion_sets_closed_flag(self, scheduler):
        """Test that wait_for_completion sets closed=True."""
        assert scheduler.closed is False

        await scheduler.wait_for_completion(timeout=0.1)

        assert scheduler.closed is True

    @pytest.mark.asyncio
    async def test_wait_for_completion_waits_for_tasks(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that wait_for_completion waits for in-flight messages."""
        completed = []

        async def slow_task():
            await asyncio.sleep(0.2)
            completed.append(1)

        task = asyncio.create_task(slow_task())
        scheduler.register_task_execution(task, mock_message)

        result = await scheduler.wait_for_completion(timeout=1.0)

        assert result is True
        assert len(completed) == 1
        assert scheduler.closed is True

    @pytest.mark.asyncio
    async def test_wait_for_completion_returns_true_on_success(
        self, scheduler
    ):
        """Test that wait_for_completion returns True when all complete."""

        result = await scheduler.wait_for_completion(timeout=1.0)

        assert result is True
        assert scheduler.closed is True

    @pytest.mark.asyncio
    async def test_wait_for_completion_returns_false_on_timeout(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that wait_for_completion returns False on timeout."""

        async def very_slow_task():
            await asyncio.sleep(10.0)

        task = asyncio.create_task(very_slow_task())
        scheduler.register_task_execution(task, mock_message)

        result = await scheduler.wait_for_completion(timeout=0.1)

        assert result is False
        assert scheduler.closed is True

        task.cancel()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_pending_handles(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that shutdown cancels pending handles."""

        callback_executed = []

        def callback(msg):
            callback_executed.append(1)

        scheduler.schedule(callback, mock_message)

        dropped = scheduler.shutdown(await_msg_callbacks=True)

        assert len(dropped) >= 1

        await asyncio.sleep(0.1)
        assert len(callback_executed) == 0

    @pytest.mark.asyncio
    async def test_shutdown_returns_dropped_messages(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that shutdown returns list of dropped messages."""

        async def task_coro():
            await asyncio.sleep(1.0)

        task = asyncio.create_task(task_coro())
        scheduler.register_task_execution(task, mock_message)

        dropped = scheduler.shutdown(await_msg_callbacks=False)

        assert len(dropped) == 1
        assert mock_message in dropped

        task.cancel()


class TestAsyncSchedulerBackwardCompatibility:
    """Test that existing scheduler functionality still works."""

    @pytest.mark.asyncio
    async def test_schedule_calls_callback(
        self, scheduler: AsyncScheduler, mock_message: MagicMock
    ):
        """Test that schedule still calls the callback."""
        callback_called = []

        def callback(msg):
            callback_called.append(msg)

        scheduler.schedule(callback, mock_message)

        # Give callback time to execute (increased delay)
        await asyncio.sleep(0.2)

        assert len(callback_called) == 1
        assert callback_called[0] == mock_message

    @pytest.mark.asyncio
    async def test_queue_property_returns_queue(self, scheduler):
        """Test that queue property works."""
        import queue

        q = scheduler.queue

        assert isinstance(q, queue.Queue)
        assert q is scheduler._queue
