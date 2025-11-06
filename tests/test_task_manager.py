from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from fastpubsub.concurrency.manager import AsyncTaskManager
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry

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


    def test_ready_check(self, task: MagicMock):
        mock_subscriber = MagicMock()
        mock_subscriber.ready = True
        mock_subscriber.name = "sub_name"
        task.return_value.subscriber = mock_subscriber
        task.return_value.task_ready.return_value = True

        task_manager = AsyncTaskManager()
        task_manager.create_task(mock_subscriber)
        task_manager.start()

        readiness = task_manager.ready()

        assert isinstance(readiness, dict)
        assert len(readiness) == 1
        assert mock_subscriber.name in readiness
        assert readiness[mock_subscriber.name]

    def test_start(self, task: MagicMock):
        task_manager = AsyncTaskManager()
        task_manager.create_task(MagicMock())
        task_manager.start()

        task.assert_called_once()
        task.return_value.start.assert_called_once()

    def test_shutdown(self, task: MagicMock):
        task_manager = AsyncTaskManager()
        task_manager.create_task(MagicMock())
        task_manager.start()
        task_manager.shutdown()

        task.assert_called_once()
        task.return_value.start.assert_called_once()

"""
class TestPubSubPollTask:
    @pytest.fixture
    def pubsub_client(self) -> Generator[MagicMock]:
        with patch(f"{PUBSUB_POLL_TASK_MODULE_PATH}.PubSubClient") as pubsub_client:
            yield pubsub_client.return_value

    @pytest.fixture(autouse=True)
    def create_task(self) -> Generator[MagicMock]:
        with patch(
            f"{PUBSUB_POLL_TASK_MODULE_PATH}.asyncio.create_task", new_callable=MagicMock
        ) as create_task:
            yield create_task

    @pytest.fixture
    def received_message(self) -> MagicMock:
        message = MagicMock()
        message.message.message_id = "123"
        message.message.data = b"some_bytes"
        message.message.attributes = {
            "key": "value",
        }
        message.delivery_attempt = 2
        return message

    @pytest.fixture
    def received_message_first_attempt(self, received_message: MagicMock) -> MagicMock:
        received_message.delivery_attempt = None
        return received_message

    @pytest.mark.asyncio
    async def test_task_ready(self):
        task = PubSubPullTask(MagicMock())
        assert not task.task_ready()

        task.ready = True
        assert task.task_ready()

    @pytest.mark.asyncio
    async def test_task_alive(self):
        task = PubSubPullTask(MagicMock())
        assert not task.task_alive()

        task.running = True
        assert task.task_alive()

        task.shutdown()
        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_deserialize_message(self, received_message: MagicMock):
        task = PubSubPullTask(MagicMock())

        message = await task._deserialize_message(received_message)

        assert isinstance(message, Message)
        assert message.id == received_message.message.message_id
        assert message.data == received_message.message.data
        assert message.attributes == received_message.message.attributes
        assert message.size == len(received_message.message.data)
        assert message.delivery_attempt == received_message.delivery_attempt

    @pytest.mark.asyncio
    async def test_deserialize_message_without_delivery_attempt(
        self, received_message_first_attempt: MagicMock
    ):
        task = PubSubPullTask(MagicMock())

        message = await task._deserialize_message(received_message_first_attempt)

        assert isinstance(message, Message)
        assert message.id == received_message_first_attempt.message.message_id
        assert message.data == received_message_first_attempt.message.data
        assert message.attributes == received_message_first_attempt.message.attributes
        assert message.size == len(received_message_first_attempt.message.data)
        assert message.delivery_attempt == 0

    @pytest.mark.parametrize(
        ["exception", "expected_liveness"],
        [
            [Aborted(None), True],
            [DeadlineExceeded(None), True],
            [GatewayTimeout(None), True],
            [InternalServerError(None), True],
            [ResourceExhausted(None), True],
            [ServiceUnavailable(None), True],
            [Unknown(None), True],
            [ValueError(None), True],
            [Cancelled(None), False],
            [InvalidArgument(None), False],
            [NotFound(None), False],
            [PermissionDenied(None), False],
            [Unauthenticated(None), False],
            [Unauthorized(None), False],
        ],
    )
    @pytest.mark.asyncio
    async def test_on_exception_handle(self, exception: Exception, expected_liveness: bool):
        task = PubSubPullTask(MagicMock())
        task.running = True
        task.ready = True

        task._on_exception(exception)

        assert not task.task_ready()
        assert task.task_alive() == expected_liveness

    @pytest.mark.asyncio
    async def test_start_task_cancelled_exception(self, pubsub_client: MagicMock):
        pubsub_client.pull.side_effect = CancelledError(None)

        task = PubSubPullTask(MagicMock())
        with pytest.raises(CancelledError):
            await task.start()

        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_start_task_on_exception(self, pubsub_client: MagicMock):
        pubsub_client.pull.side_effect = Cancelled(None)

        task = PubSubPullTask(MagicMock())
        await task.start()
        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_consume_messages_empty_messages(
        self, pubsub_client: MagicMock, create_task: MagicMock
    ):
        pubsub_client.pull = AsyncMock(return_value=[])
        task = PubSubPullTask(MagicMock())
        task._consume = MagicMock()

        await task._consume_messages()
        assert task.task_ready()
        create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_task_on_messages(
        self,
        pubsub_client: MagicMock,
        received_message: MagicMock,
        received_message_first_attempt: MagicMock,
        create_task: MagicMock,
    ):
        received_messages = [received_message, received_message_first_attempt]
        pubsub_client.pull = AsyncMock(return_value=received_messages)
        task = PubSubPullTask(MagicMock())
        task._consume = MagicMock()

        await task._consume_messages()
        assert task.task_ready()
        assert create_task.call_count == 2

    @pytest.mark.asyncio
    async def test_consume_process_message_successfully(
        self, pubsub_client: MagicMock, received_message: MagicMock
    ):
        pubsub_client.ack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock()
        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber._build_callstack = build_callstack_mock

        task = PubSubPullTask(subscriber)
        await task._consume(received_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once()
        pubsub_client.ack.assert_called_once_with(
            [received_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_drop_message(self, pubsub_client: MagicMock, received_message: MagicMock):
        pubsub_client.ack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Drop())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber._build_callstack = build_callstack_mock

        task = PubSubPullTask(subscriber)
        await task._consume(received_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once()
        pubsub_client.ack.assert_called_once_with(
            [received_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_retry_message(
        self, pubsub_client: MagicMock, received_message: MagicMock
    ):
        pubsub_client.nack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Retry())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber._build_callstack = build_callstack_mock

        task = PubSubPullTask(subscriber)
        await task._consume(received_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once()
        pubsub_client.nack.assert_called_once_with(
            [received_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_unhandled_exception_on_message(
        self, pubsub_client: MagicMock, received_message: MagicMock
    ):
        pubsub_client.nack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Exception())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber._build_callstack = build_callstack_mock

        task = PubSubPullTask(subscriber)
        await task._consume(received_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once()
        pubsub_client.nack.assert_called_once_with(
            [received_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.ack.assert_not_called()
"""