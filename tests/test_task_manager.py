from asyncio import CancelledError
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import (
    Aborted,
    Cancelled,
    DeadlineExceeded,
    GatewayTimeout,
    InternalServerError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
    Unauthorized,
    Unknown,
)

from fastpubsub.concurrency.manager import AsyncTaskManager
from fastpubsub.concurrency.tasks import PubSubPollTask
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry

PUBSUB_POLL_TASK_MODULE_PATH = "fastpubsub.concurrency.tasks"
ASYNC_TASK_MANAGER_MODULE_PATH = "fastpubsub.concurrency.manager"


class TestAsyncTaskManager:
    @pytest.fixture(autouse=True)
    def create_task_group(self) -> Generator[MagicMock]:
        with patch(
            f"{ASYNC_TASK_MANAGER_MODULE_PATH}.create_task_group", spec=MagicMock
        ) as create_task_group:
            task_group_instance = AsyncMock(return_value=MagicMock())
            create_task_group.return_value.__aenter__ = task_group_instance
            yield create_task_group.return_value.__aenter__

    @pytest.mark.asyncio
    async def test_create_task(self):
        mock_subscriber = MagicMock()

        task_manager = AsyncTaskManager()
        await task_manager.create_task(mock_subscriber)

        task = task_manager._tasks.pop(0)
        assert task.subscriber == mock_subscriber

    @pytest.mark.asyncio
    async def test_alive_check(self):
        mock_subscriber = MagicMock()
        mock_subscriber.name = "sub_name"

        task_manager = AsyncTaskManager()
        await task_manager.create_task(mock_subscriber)

        liveness = await task_manager.alive()

        assert isinstance(liveness, dict)
        assert len(liveness) == 1
        assert mock_subscriber.name in liveness
        assert not liveness[mock_subscriber.name]

    @pytest.mark.asyncio
    async def test_ready_check(self):
        mock_subscriber = MagicMock()
        mock_subscriber.ready = True
        mock_subscriber.name = "sub_name"

        task_manager = AsyncTaskManager()
        await task_manager.create_task(mock_subscriber)

        readiness = await task_manager.ready()

        assert isinstance(readiness, dict)
        assert len(readiness) == 1
        assert mock_subscriber.name in readiness
        assert not readiness[mock_subscriber.name]

    @pytest.mark.asyncio
    async def test_start(self, create_task_group: MagicMock):
        task_manager = AsyncTaskManager()
        await task_manager.create_task(MagicMock())
        await task_manager.start()

        create_task_group.assert_called_once()
        create_task_group.return_value.start_soon.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown(self):
        task_group = AsyncMock()

        task_manager = AsyncTaskManager()
        task_manager._task_group = task_group
        await task_manager.shutdown()

        task_group.__aexit__.assert_called_once()


class TestPubSubPollTask:
    @pytest.fixture
    def pubsub_client(self) -> Generator[MagicMock]:
        with patch(f"{PUBSUB_POLL_TASK_MODULE_PATH}.PubSubClient") as pubsub_client:
            yield pubsub_client.return_value

    @pytest.fixture(autouse=True)
    def create_task_group(self) -> Generator[MagicMock]:
        with patch(f"{PUBSUB_POLL_TASK_MODULE_PATH}.create_task_group") as create_task_group:
            instance = create_task_group.return_value.__aenter__.return_value
            instance.cancel_scope.cancel = MagicMock()
            yield create_task_group

    @pytest.fixture
    def received_message(self) -> MagicMock:
        message = MagicMock()
        message.ack_id = "abc"
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

    @pytest.fixture
    def default_message(self) -> Message:
        return Message(
            id="123", size=3, data=b"abc", ack_id="321", attributes={}, delivery_attempt=0
        )

    @pytest.mark.asyncio
    async def test_task_ready(self):
        task = PubSubPollTask(MagicMock())
        assert not task.task_ready()

        task.ready = True
        assert task.task_ready()

    @pytest.mark.asyncio
    async def test_task_alive(self):
        task = PubSubPollTask(MagicMock())
        assert not task.task_alive()

        task.running = True
        assert task.task_alive()

        task.shutdown()
        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_deserialize_message(self, received_message: MagicMock):
        task = PubSubPollTask(MagicMock())

        message = await task._deserialize_message(received_message)

        assert isinstance(message, Message)
        assert message.id == received_message.message.message_id
        assert message.data == received_message.message.data
        assert message.attributes == received_message.message.attributes
        assert message.ack_id == received_message.ack_id
        assert message.size == len(received_message.message.data)
        assert message.delivery_attempt == received_message.delivery_attempt

    @pytest.mark.asyncio
    async def test_deserialize_message_without_delivery_attempt(
        self, received_message_first_attempt: MagicMock
    ):
        task = PubSubPollTask(MagicMock())

        message = await task._deserialize_message(received_message_first_attempt)

        assert isinstance(message, Message)
        assert message.id == received_message_first_attempt.message.message_id
        assert message.data == received_message_first_attempt.message.data
        assert message.attributes == received_message_first_attempt.message.attributes
        assert message.ack_id == received_message_first_attempt.ack_id
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
        task = PubSubPollTask(MagicMock())
        task.running = True
        task.ready = True

        task._on_exception(exception)

        assert not task.task_ready()
        assert task.task_alive() == expected_liveness

    @pytest.mark.asyncio
    async def test_start_task_cancelled_exception(self, pubsub_client: MagicMock):
        pubsub_client.pull.side_effect = CancelledError(None)

        task = PubSubPollTask(MagicMock())
        with pytest.raises(CancelledError):
            await task.start()

        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_start_task_on_exception(self, pubsub_client: MagicMock):
        pubsub_client.pull.side_effect = Cancelled(None)

        task = PubSubPollTask(MagicMock())
        await task.start()
        assert not task.task_alive()

    @pytest.mark.asyncio
    async def test_consume_messages_empty_messages(self, pubsub_client: MagicMock):
        pubsub_client.pull = AsyncMock(return_value=[])
        task = PubSubPollTask(MagicMock())

        mock_task_group = MagicMock()
        await task._consume_messages(mock_task_group)

        assert task.task_ready()
        mock_task_group.start_soon.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_task_on_messages(
        self,
        pubsub_client: MagicMock,
        received_message: MagicMock,
        received_message_first_attempt: MagicMock,
    ):
        received_messages = [received_message, received_message_first_attempt]
        pubsub_client.pull = AsyncMock(return_value=received_messages)
        task = PubSubPollTask(MagicMock())

        mock_task_group = MagicMock()
        await task._consume_messages(mock_task_group)
        assert task.task_ready()

        assert mock_task_group.start_soon.call_count == 2

    @pytest.mark.asyncio
    async def test_consume_process_message_successfully(
        self, pubsub_client: MagicMock, default_message: Message
    ):
        pubsub_client.ack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock()

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber.build_callstack = build_callstack_mock

        task = PubSubPollTask(subscriber)
        await task._consume(default_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once_with(default_message)
        pubsub_client.ack.assert_called_once_with(
            [default_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_drop_message(self, pubsub_client: MagicMock, default_message: Message):
        pubsub_client.ack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Drop())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber.build_callstack = build_callstack_mock

        task = PubSubPollTask(subscriber)
        await task._consume(default_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once_with(default_message)
        pubsub_client.ack.assert_called_once_with(
            [default_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.nack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_retry_message(self, pubsub_client: MagicMock, default_message: Message):
        pubsub_client.nack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Retry())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber.build_callstack = build_callstack_mock

        task = PubSubPollTask(subscriber)
        await task._consume(default_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once_with(default_message)
        pubsub_client.nack.assert_called_once_with(
            [default_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.ack.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_unhandled_exception_on_message(
        self, pubsub_client: MagicMock, default_message: Message
    ):
        pubsub_client.nack = AsyncMock()
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Exception())

        build_callstack_mock = AsyncMock(return_value=callstack_mock)

        subscriber = MagicMock()
        subscriber.build_callstack = build_callstack_mock

        task = PubSubPollTask(subscriber)
        await task._consume(default_message)

        build_callstack_mock.assert_called_once()
        callstack_mock.on_message.assert_called_once_with(default_message)
        pubsub_client.nack.assert_called_once_with(
            [default_message.ack_id], subscriber.subscription_name
        )
        pubsub_client.ack.assert_not_called()
