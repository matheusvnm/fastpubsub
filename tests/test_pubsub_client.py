from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus

from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.clients.sub import CallbackHandler, PubSubSubscriberClient
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import Drop, FastPubSubException, Retry
from fastpubsub.pubsub.subscriber import Subscriber


@pytest.fixture
def subscriber():
    async def dummy_handler():
        pass

    subscriber = Subscriber(
        func=dummy_handler,
        topic_name="test-topic",
        subscription_name="test-subscription",
        retry_policy=MessageRetryPolicy(min_backoff_delay_secs=10, max_backoff_delay_secs=60),
        lifecycle_policy=LifecyclePolicy(autocreate=True, autoupdate=False),
        delivery_policy=MessageDeliveryPolicy(
            filter_expression="",
            ack_deadline_seconds=60,
            enable_message_ordering=False,
            enable_exactly_once_delivery=False,
        ),
        control_flow_policy=MessageControlFlowPolicy(max_bytes=10, max_messages=1),
        dead_letter_policy=DeadLetterPolicy(topic_name="dlt", max_delivery_attempts=5),
    )
    subscriber.set_project_id("test-project")
    subscriber.build_callstack = MagicMock()
    return subscriber


class TestPubSubPublisherClient:
    @patch("fastpubsub.clients.pub.PublisherClient")
    @patch("fastpubsub.clients.pub.SubscriberClient")
    def test_create_topic(self, mock_subscriber_client, mock_publisher_client):
        client = PubSubPublisherClient(project_id="test-project", topic_name="test-topic")
        client.create_topic()

        mock_publisher_client.return_value.create_topic.assert_called_once()
        mock_subscriber_client.return_value.__enter__.return_value.create_subscription.assert_called_once()

    @patch("fastpubsub.clients.pub.PublisherClient")
    def test_publish(self, mock_publisher_client):
        client = PubSubPublisherClient(project_id="test-project", topic_name="test-topic")
        client.publish(data=b"test-data", ordering_key=None, attributes=None)

        mock_publisher_client.return_value.publish.assert_called_once()

    @patch("fastpubsub.clients.pub.PublisherClient")
    def test_publish_failure(self, mock_publisher_client):
        result = Future()
        result.set_exception(ValueError)
        mock_publisher_client.return_value.publish.return_value = result

        client = PubSubPublisherClient(project_id="test-project", topic_name="test-topic")

        with pytest.raises(ValueError):
            client.publish(data=b"test-data", ordering_key=None, attributes=None)


class TestPubSubSubscriberClient:
    @pytest.fixture(autouse=True)
    def mock_subscriber_client(self):
        with patch("fastpubsub.clients.sub.SubscriberClient") as mock:
            mock.subscription_path.return_value = "some_sub_path"
            mock.topic_path.return_value = "some_topic_path"
            yield mock

    def test_create_subscription(self, subscriber: Subscriber, mock_subscriber_client: MagicMock):
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber)

        mock_subscriber_client.return_value.__enter__.return_value.create_subscription.assert_called_once()

    def test_update_subscription(self, subscriber: Subscriber, mock_subscriber_client: MagicMock):
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber)
        mock_subscriber_client.return_value.__enter__.return_value.update_subscription.assert_called_once()

    def test_subscribe(self, subscriber: Subscriber, mock_subscriber_client: MagicMock):
        client = PubSubSubscriberClient()
        streaming_pull_future = MagicMock()
        mock_subscriber_client.return_value.__enter__.return_value.subscribe.return_value = (
            streaming_pull_future
        )

        with client.subscribe(subscriber) as future:
            assert future == streaming_pull_future

        mock_subscriber_client.return_value.__enter__.return_value.subscribe.assert_called_once()

    def test_subscribe_exception(self, subscriber: Subscriber, mock_subscriber_client: MagicMock):
        client = PubSubSubscriberClient()
        streaming_pull_future = MagicMock()
        streaming_pull_future.result.side_effect = Exception
        mock_subscriber_client.return_value.__enter__.return_value.subscribe.return_value = (
            streaming_pull_future
        )

        with client.subscribe(subscriber):
            pass

        streaming_pull_future.cancel.assert_called_once()

    def test_subscribe_keyboard_interrupt(
        self, subscriber: Subscriber, mock_subscriber_client: MagicMock
    ):
        client = PubSubSubscriberClient()
        streaming_pull_future = MagicMock()
        streaming_pull_future.result.side_effect = KeyboardInterrupt
        mock_subscriber_client.return_value.__enter__.return_value.subscribe.return_value = (
            streaming_pull_future
        )

        with client.subscribe(subscriber):
            pass

        streaming_pull_future.cancel.assert_called_once()

    def test_update_subscription_not_found(
        self, subscriber: Subscriber, mock_subscriber_client: MagicMock
    ):
        from google.api_core.exceptions import NotFound

        mock_subscriber_client.return_value.__enter__.return_value.update_subscription.side_effect = NotFound(
            "test"
        )
        client = PubSubSubscriberClient()
        with pytest.raises(FastPubSubException):
            client.update_subscription(subscriber)

    @patch("os.getenv", return_value="1")
    def test_update_subscription_emulator(
        self, mock_getenv, subscriber: Subscriber, mock_subscriber_client: MagicMock
    ):
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber)
        mock_subscriber_client.return_value.__enter__.return_value.update_subscription.assert_called_once()


class TestCallbackHandler:
    @patch("fastpubsub.clients.sub.asyncio")
    def test_handle_success(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.ack_with_response.assert_called_once()

    @patch("fastpubsub.clients.sub.asyncio")
    def test_handle_drop(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        mock_asyncio.run.side_effect = Drop
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.ack_with_response.assert_called_once()

    @patch("fastpubsub.clients.sub.asyncio")
    def test_handle_retry(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        mock_asyncio.run.side_effect = Retry
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.nack_with_response.assert_called_once()

    @patch("fastpubsub.clients.sub.asyncio")
    def test_handle_exception(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        mock_asyncio.run.side_effect = Exception
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.nack_with_response.assert_called_once()

    @patch("fastpubsub.clients.sub.get_apm_provider")
    def test_start_apm_transaction(self, mock_get_apm_provider, subscriber: Subscriber):
        handler = CallbackHandler(subscriber)
        with handler._start_apm_transaction():
            pass
        mock_get_apm_provider.return_value.background_transaction.assert_called_once()

    def test_translate_message(self, subscriber: Subscriber):
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.message_id = "123"
        message.size = 100
        message.data = b"data"
        message.attributes = {}
        message.delivery_attempt = 1

        translated_message = handler._translate_message(message)
        assert translated_message.id == "123"

    def test_wait_acknowledge_response_timeout(self, subscriber: Subscriber):
        handler = CallbackHandler(subscriber)
        future = MagicMock()
        future.result.side_effect = TimeoutError

        handler._wait_acknowledge_response(future)
        future.result.assert_called_once_with(timeout=60)

    @pytest.mark.parametrize(
        "error_code",
        [
            AcknowledgeStatus.PERMISSION_DENIED,
            AcknowledgeStatus.FAILED_PRECONDITION,
            AcknowledgeStatus.INVALID_ACK_ID,
            AcknowledgeStatus.OTHER,
        ],
    )
    def test_on_acknowledge_failed(self, subscriber: Subscriber, error_code: AcknowledgeStatus):
        handler = CallbackHandler(subscriber)
        exception = AcknowledgeError(info="Test error", error_code=error_code)
        handler._on_acknowledge_failed(exception)
