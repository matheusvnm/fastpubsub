from unittest.mock import MagicMock

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

from fastpubsub.concurrency.tasks import PubSubPollTask
from fastpubsub.datastructures import Message


class TestAsyncTaskManager:
    # TODO:
    ## Testar create task
    ## Testar start (all tasks)
    ## Testar alive
    ## Testar ready
    ## Testar shutdown

    ...


class TestPubSubPollTask:
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
    async def test_deserialize_message(self):
        task = PubSubPollTask(MagicMock())

        received_message = MagicMock()
        received_message.ack_id = "abc"
        received_message.message.message_id = "123"
        received_message.message.data = b"some_bytes"
        received_message.message.attributes = {
            "key": "value",
        }
        received_message.delivery_attempt = 2

        message = await task._deserialize_message(received_message)

        assert isinstance(message, Message)
        assert message.id == received_message.message.message_id
        assert message.data == received_message.message.data
        assert message.attributes == received_message.message.attributes
        assert message.ack_id == received_message.ack_id
        assert message.size == len(received_message.message.data)
        assert message.delivery_attempt == received_message.delivery_attempt

    @pytest.mark.asyncio
    async def test_deserialize_message_without_delivery_attempt(self):
        task = PubSubPollTask(MagicMock())

        received_message = MagicMock()
        received_message.ack_id = "abc"
        received_message.message.message_id = "123"
        received_message.message.data = b"some_bytes"
        received_message.message.attributes = {
            "key": "value",
        }
        received_message.delivery_attempt = None

        message = await task._deserialize_message(received_message)

        assert isinstance(message, Message)
        assert message.id == received_message.message.message_id
        assert message.data == received_message.message.data
        assert message.attributes == received_message.message.attributes
        assert message.ack_id == received_message.ack_id
        assert message.size == len(received_message.message.data)
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

    # TODO:
    ## Testar start tasks

    ...
