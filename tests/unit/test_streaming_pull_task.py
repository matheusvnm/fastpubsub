"""Tests for PubSubStreamingPullTask and MessageMapper."""

from __future__ import annotations

from collections.abc import Generator
from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture

from fastpubsub.concurrency.tasks import MessageMapper, PubSubStreamingPullTask
from fastpubsub.datastructures import PullMessage
from fastpubsub.exceptions import Drop, Retry

TASKS_MODULE_PATH = "fastpubsub.concurrency.tasks"
CLIENTS_MODULE_PATH = "fastpubsub.clients.pubsub"


class TestMessageMapper:
    @pytest.fixture
    def subscriber(self) -> MagicMock:
        """Create a mock subscriber."""
        subscriber = MagicMock()
        subscriber.project_id = "test-project"
        subscriber.topic_name = "test-topic"
        subscriber.name = "test-subscriber"
        return subscriber

    @pytest.fixture
    def mapper(self, subscriber: MagicMock) -> MessageMapper:
        """Create a MessageMapper instance."""
        return MessageMapper(subscriber)

    @pytest.fixture
    def pubsub_message(self) -> MagicMock:
        """Create a mock PubSub message."""
        message = MagicMock()
        message.message_id = "msg-123"
        message.data = b'{"key": "value"}'
        message.size = 16
        message.attributes = {"attr1": "value1", "attr2": "value2"}
        message.delivery_attempt = 2
        return message

    def test_convert_message_with_delivery_attempt(
        self, mapper: MessageMapper, pubsub_message: MagicMock
    ):
        """Test converting a PubSub message with delivery attempt set."""
        result = mapper.convert(pubsub_message)

        assert isinstance(result, PullMessage)
        assert result.id == "msg-123"
        assert result.data == b'{"key": "value"}'
        assert result.size == 16
        assert result.attributes == {"attr1": "value1", "attr2": "value2"}
        assert result.delivery_attempt == 2
        assert result.project_id == "test-project"
        assert result.topic_name == "test-topic"
        assert result.subscriber_name == "test-subscriber"

    def test_convert_message_without_delivery_attempt(
        self, mapper: MessageMapper, pubsub_message: MagicMock
    ):
        """Test converting a PubSub message without delivery attempt (first attempt)."""
        pubsub_message.delivery_attempt = None

        result = mapper.convert(pubsub_message)

        assert result.delivery_attempt == 0

    def test_convert_message_first_delivery(self, mapper: MessageMapper, pubsub_message: MagicMock):
        """Test converting a message on first delivery attempt."""
        pubsub_message.delivery_attempt = 0

        result = mapper.convert(pubsub_message)

        assert result.delivery_attempt == 0


class TestPubSubStreamingPullTask:
    @pytest.fixture
    def subscriber(self) -> MagicMock:
        """Create a mock subscriber."""
        subscriber = MagicMock()
        subscriber.project_id = "test-project"
        subscriber.topic_name = "test-topic"
        subscriber.subscription_name = "test-subscription"
        subscriber.name = "test-subscriber"
        subscriber.control_flow_policy.max_messages = 100
        return subscriber

    @pytest.fixture
    def mock_pubsub_client(self) -> Generator[MagicMock]:
        """Create a mock PubSubClient class.

        PubSubClient is imported inside __init__, so we patch it at the source.
        """
        with patch(f"{CLIENTS_MODULE_PATH}.PubSubClient") as mock_class:
            yield mock_class

    @pytest.fixture
    def streaming_pull_future(self) -> MagicMock:
        """Create a mock StreamingPullFuture.

        Uses spec=StreamingPullFuture so isinstance() checks pass.
        """
        future = MagicMock(spec=StreamingPullFuture)
        future.running.return_value = True
        future.done.return_value = False
        return future

    @pytest.fixture
    def pubsub_message(self) -> MagicMock:
        """Create a mock received PubSub message."""
        message = MagicMock()
        message.message_id = "msg-123"
        message.data = b'{"key": "value"}'
        message.size = 16
        message.attributes = {"attr1": "value1"}
        message.delivery_attempt = 1
        return message

    @pytest.mark.asyncio
    async def test_start_subscribes_to_topic(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test that start() initiates subscription."""
        mock_client_instance = mock_pubsub_client.return_value
        mock_client_instance.subscribe = AsyncMock(return_value=streaming_pull_future)

        task = PubSubStreamingPullTask(subscriber)
        await task.start()

        mock_client_instance.subscribe.assert_called_once_with(
            callback=task._on_message,
            subscription_name="test-subscription",
            max_messages=100,
        )
        assert task.polling is streaming_pull_future

    @pytest.mark.asyncio
    async def test_task_ready_without_task(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test task_ready returns False when task is not started."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = None

        assert task.task_ready() is False

    @pytest.mark.asyncio
    async def test_task_ready_when_running(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test task_ready returns True when task is running."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.running.return_value = True

        assert task.task_ready() is True

    @pytest.mark.asyncio
    async def test_task_ready_when_not_running(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test task_ready returns False when task is not running."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.running.return_value = False

        assert task.task_ready() is False

    @pytest.mark.asyncio
    async def test_task_alive_without_task(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test task_alive returns False when task is not started."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = None

        assert task.task_alive() is False

    @pytest.mark.asyncio
    async def test_task_alive_when_not_done(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test task_alive returns True when task is not done."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.done.return_value = False

        assert task.task_alive() is True

    @pytest.mark.asyncio
    async def test_task_alive_when_done(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test task_alive returns False when task is done."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.done.return_value = True

        assert task.task_alive() is False

    @pytest.mark.asyncio
    async def test_shutdown_cancels_task(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test shutdown cancels the running task."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.running.return_value = True

        await task.shutdown()

        streaming_pull_future.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_shutdown_does_nothing_when_not_running(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, streaming_pull_future: MagicMock
    ):
        """Test shutdown does not cancel when task is not running."""
        task = PubSubStreamingPullTask(subscriber)
        task.polling = streaming_pull_future
        streaming_pull_future.running.return_value = False

        await task.shutdown()

        streaming_pull_future.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_successful_message(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, pubsub_message: MagicMock
    ):
        """Test consuming a message successfully acks it."""
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(return_value="result")
        subscriber._build_callstack.return_value = callstack_mock

        ack_future: Future[None] = Future()
        ack_future.set_result(None)
        pubsub_message.ack_with_response.return_value = ack_future

        task = PubSubStreamingPullTask(subscriber)
        result = await task._consume(pubsub_message)

        assert result == "result"
        subscriber._build_callstack.assert_called_once()
        callstack_mock.on_message.assert_called_once()
        pubsub_message.ack_with_response.assert_called_once()

    @pytest.mark.asyncio
    async def test_consume_drop_exception_acks_message(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, pubsub_message: MagicMock
    ):
        """Test that Drop exception results in message being acked."""
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Drop())
        subscriber._build_callstack.return_value = callstack_mock

        ack_future: Future[None] = Future()
        ack_future.set_result(None)
        pubsub_message.ack_with_response.return_value = ack_future

        task = PubSubStreamingPullTask(subscriber)
        await task._consume(pubsub_message)

        pubsub_message.ack_with_response.assert_called_once()
        pubsub_message.nack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_retry_exception_nacks_message(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, pubsub_message: MagicMock
    ):
        """Test that Retry exception results in message being nacked."""
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=Retry())
        subscriber._build_callstack.return_value = callstack_mock

        nack_future: Future[None] = Future()
        nack_future.set_result(None)
        pubsub_message.nack_with_response.return_value = nack_future

        task = PubSubStreamingPullTask(subscriber)
        await task._consume(pubsub_message)

        pubsub_message.nack_with_response.assert_called_once()
        pubsub_message.ack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_consume_unhandled_exception_nacks_message(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, pubsub_message: MagicMock
    ):
        """Test that unhandled exception results in message being nacked."""
        callstack_mock = MagicMock()
        callstack_mock.on_message = AsyncMock(side_effect=ValueError("Unhandled error"))
        subscriber._build_callstack.return_value = callstack_mock

        nack_future: Future[None] = Future()
        nack_future.set_result(None)
        pubsub_message.nack_with_response.return_value = nack_future

        task = PubSubStreamingPullTask(subscriber)
        await task._consume(pubsub_message)

        pubsub_message.nack_with_response.assert_called_once()
        pubsub_message.ack_with_response.assert_not_called()

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_success(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test successful acknowledge response."""
        future: Future[None] = Future()
        future.set_result(None)

        task = PubSubStreamingPullTask(subscriber)

        # Should not raise
        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_permission_denied(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test handling permission denied acknowledge error."""
        error = AcknowledgeError(AcknowledgeStatus.PERMISSION_DENIED, None)
        future: Future[None] = Future()
        future.set_exception(error)

        task = PubSubStreamingPullTask(subscriber)

        # Should not raise, just log
        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_failed_precondition(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test handling failed precondition acknowledge error."""
        error = AcknowledgeError(AcknowledgeStatus.FAILED_PRECONDITION, None)
        future: Future[None] = Future()
        future.set_exception(error)

        task = PubSubStreamingPullTask(subscriber)

        # Should not raise, just log
        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_invalid_ack_id(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test handling invalid ack id error."""
        error = AcknowledgeError(AcknowledgeStatus.INVALID_ACK_ID, None)
        future: Future[None] = Future()
        future.set_exception(error)

        task = PubSubStreamingPullTask(subscriber)

        # Should not raise, just log
        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_wait_acknowledge_response_other_error(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock
    ):
        """Test handling other acknowledge errors."""
        error = AcknowledgeError(AcknowledgeStatus.OTHER, None)
        future: Future[None] = Future()
        future.set_exception(error)

        task = PubSubStreamingPullTask(subscriber)

        # Should not raise, just log
        await task._wait_acknowledge_response(future)

    @pytest.mark.asyncio
    async def test_on_message_creates_task(
        self, subscriber: MagicMock, mock_pubsub_client: MagicMock, pubsub_message: MagicMock
    ):
        """Test that _on_message creates an asyncio task."""
        task = PubSubStreamingPullTask(subscriber)
        task._consume = AsyncMock()

        with patch.object(task.loop, "create_task") as mock_create_task:
            task._on_message(pubsub_message)

            mock_create_task.assert_called_once()
