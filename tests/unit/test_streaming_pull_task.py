"""Unit tests for PubSubStreamingPullTask."""

import asyncio
from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture

from fastpubsub.concurrency.tasks import MessageMapper, PubSubStreamingPullTask
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry


@pytest.fixture
def mock_subscriber():
    """Create mock Subscriber."""
    subscriber = MagicMock()
    subscriber.project_id = "test-project"
    subscriber.topic_name = "test-topic"
    subscriber.name = "test-subscriber"
    subscriber.subscription_name = "test-subscription"
    subscriber._build_callstack = MagicMock()
    subscriber.control_flow_policy.max_messages = 10
    return subscriber


@pytest.fixture
def mock_pubsub_message():
    """Create mock PubSubMessage."""
    msg = MagicMock()
    msg.message_id = "msg-123"
    msg.data = b"test data"
    msg.size = 9
    msg.attributes = {"key": "value"}
    msg.delivery_attempt = 2
    msg.ack = MagicMock()
    msg.nack = MagicMock()
    msg.ack_with_response = MagicMock()
    msg.nack_with_response = MagicMock()
    return msg


class TestMessageMapper:
    """Test the MessageMapper class."""

    def test_message_mapper_convert(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test MessageMapper.convert() correctly maps PubSubMessage to Message."""
        mapper = MessageMapper(mock_subscriber)

        result = mapper.convert(mock_pubsub_message)

        assert isinstance(result, Message)
        assert result.id == "msg-123"
        assert result.data == b"test data"
        assert result.size == 9
        assert result.attributes == {"key": "value"}
        assert result.delivery_attempt == 2
        assert result.project_id == "test-project"
        assert result.topic_name == "test-topic"
        assert result.subscriber_name == "test-subscriber"

    def test_message_mapper_convert_without_delivery_attempt(
        self, mock_subscriber, mock_pubsub_message
    ):
        """Test delivery attempt defaults to 0 when None."""
        mock_pubsub_message.delivery_attempt = None
        mapper = MessageMapper(mock_subscriber)

        result = mapper.convert(mock_pubsub_message)

        assert result.delivery_attempt == 0


class TestPubSubStreamingPullTask:
    """Test PubSubStreamingPullTask functionality."""

    @pytest.fixture(autouse=True)
    def mock_get_loop(self):
        with patch("fastpubsub.concurrency.tasks.asyncio.get_running_loop") as get_loop:
            yield get_loop

    @pytest.fixture
    def mock_streaming_pull_future(self) -> MagicMock:
        """Create mock StreamingPullFuture."""
        future = MagicMock()
        future.running = MagicMock(return_value=True)
        future.done = MagicMock(return_value=False)
        future.cancel = MagicMock()
        future.result = MagicMock()
        return future

    @pytest.mark.asyncio
    async def test_task_ready(self, mock_subscriber: MagicMock):
        """Test task_ready returns correct state."""

        task = PubSubStreamingPullTask(mock_subscriber)
        assert task.task_ready() is False

        mock_future = MagicMock(spec=StreamingPullFuture)
        mock_future.running.return_value = True
        task.task = mock_future

        assert task.task_ready() is True

        mock_future.running.return_value = False

        assert task.task_ready() is False

    @pytest.mark.asyncio
    async def test_task_alive(self, mock_subscriber: MagicMock):
        """Test task_alive returns correct state."""

        task = PubSubStreamingPullTask(mock_subscriber)
        assert task.task_alive() is False

        mock_future = MagicMock(spec=StreamingPullFuture)
        mock_future.done.return_value = False
        task.task = mock_future

        assert task.task_alive() is True

        mock_future.done.return_value = True

        assert task.task_alive() is False

    @pytest.mark.asyncio
    async def test_on_message_creates_task(
        self, mock_get_loop: MagicMock, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test that _on_message creates a task and registers it with scheduler."""

        event_loop = asyncio.get_event_loop()
        mock_get_loop.return_value = event_loop

        task = PubSubStreamingPullTask(mock_subscriber)
        task.scheduler.register_task_execution = MagicMock()
        task._consume = AsyncMock()

        result = task._on_message(mock_pubsub_message)

        assert asyncio.iscoroutine(result.get_coro())
        task.scheduler.register_task_execution.assert_called_once()

        result.cancel()

    @pytest.mark.asyncio
    async def test_consume_process_message_successfully(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test message processed successfully, ack called, no nack."""

        task = PubSubStreamingPullTask(mock_subscriber)

        mock_callstack = MagicMock()
        mock_callstack.on_message = AsyncMock(return_value=None)
        mock_subscriber._build_callstack.return_value = mock_callstack

        ack_future = Future()
        ack_future.set_result(None)
        mock_pubsub_message.ack_with_response.return_value = ack_future

        await task._consume(mock_pubsub_message)

        mock_subscriber._build_callstack.assert_called_once()
        mock_callstack.on_message.assert_called_once()

        mock_pubsub_message.ack_with_response.assert_called_once()
        mock_pubsub_message.nack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_drop_message(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test Drop exception handled, message acked."""

        task = PubSubStreamingPullTask(mock_subscriber)

        mock_callstack = MagicMock()
        mock_callstack.on_message = AsyncMock(side_effect=Drop())
        mock_subscriber._build_callstack.return_value = mock_callstack

        ack_future = Future()
        ack_future.set_result(None)
        mock_pubsub_message.ack_with_response.return_value = ack_future

        await task._consume(mock_pubsub_message)

        mock_pubsub_message.ack_with_response.assert_called_once()
        mock_pubsub_message.nack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_retry_message(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test Retry exception handled, message nacked."""

        task = PubSubStreamingPullTask(mock_subscriber)

        mock_callstack = MagicMock()
        mock_callstack.on_message = AsyncMock(side_effect=Retry())
        mock_subscriber._build_callstack.return_value = mock_callstack

        nack_future = Future()
        nack_future.set_result(None)
        mock_pubsub_message.nack_with_response.return_value = nack_future

        await task._consume(mock_pubsub_message)

        mock_pubsub_message.nack_with_response.assert_called_once()
        mock_pubsub_message.ack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_handle_unhandled_exception_on_message(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test unhandled exception nacks message."""

        task = PubSubStreamingPullTask(mock_subscriber)

        mock_callstack = MagicMock()
        mock_callstack.on_message = AsyncMock(side_effect=ValueError("Test error"))
        mock_subscriber._build_callstack.return_value = mock_callstack

        nack_future = Future()
        nack_future.set_result(None)
        mock_pubsub_message.nack_with_response.return_value = nack_future

        await task._consume(mock_pubsub_message)

        mock_pubsub_message.nack_with_response.assert_called_once()
        mock_pubsub_message.ack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_success(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test successful ack response."""
        task = PubSubStreamingPullTask(mock_subscriber)

        future = Future()
        future.set_result(None)

        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_permission_denied(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test AcknowledgeError with PERMISSION_DENIED."""

        task = PubSubStreamingPullTask(mock_subscriber)

        future = Future()
        error = AcknowledgeError(AcknowledgeStatus.PERMISSION_DENIED, "Permission denied")
        future.set_exception(error)

        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_failed_precondition(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test AcknowledgeError with FAILED_PRECONDITION."""

        task = PubSubStreamingPullTask(mock_subscriber)

        future = Future()
        error = AcknowledgeError(AcknowledgeStatus.FAILED_PRECONDITION, "Failed precondition")
        future.set_exception(error)

        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_invalid_ack_id(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test AcknowledgeError with INVALID_ACK_ID."""

        task = PubSubStreamingPullTask(mock_subscriber)

        future = Future()
        error = AcknowledgeError(AcknowledgeStatus.INVALID_ACK_ID, "Invalid ack ID")
        future.set_exception(error)

        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_timeout(
        self, mock_subscriber: MagicMock, mock_pubsub_message: MagicMock
    ):
        """Test TimeoutError handled."""

        task = PubSubStreamingPullTask(mock_subscriber)

        future = Future()
        future.set_exception(TimeoutError("Timeout"))

        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_shutdown_waits_and_cancels_task(
        self, mock_subscriber: MagicMock, mock_streaming_pull_future: MagicMock
    ):
        """Test shutdown calls scheduler.wait_for_completion() then cancels StreamingPullFuture."""

        task = PubSubStreamingPullTask(mock_subscriber)
        task.task = mock_streaming_pull_future
        task.scheduler.wait_for_completion = AsyncMock(return_value=True)
        mock_streaming_pull_future.running.return_value = True

        await task.shutdown(timeout=30.0)

        task.scheduler.wait_for_completion.assert_called_once_with(timeout=30.0)
        mock_streaming_pull_future.cancel.assert_called_once()
        mock_streaming_pull_future.result.assert_called_once_with(timeout=30.0)
