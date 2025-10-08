from collections.abc import Generator
from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.pubsub.subscriber import Subscriber

PUBSUB_CLIENT_MODULE_PATH = "fastpubsub.clients.pubsub"


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
        control_flow_policy=MessageControlFlowPolicy(max_messages=1),
        dead_letter_policy=DeadLetterPolicy(topic_name="dlt", max_delivery_attempts=5),
    )
    subscriber.set_project_id("test-project")
    subscriber.build_callstack = AsyncMock()
    return subscriber


class TestPubSubClient:
    @pytest.fixture
    def pub_client(self) -> Generator[MagicMock]:
        with patch(f"{PUBSUB_CLIENT_MODULE_PATH}.PublisherClient") as pub_client:
            instance = pub_client.return_value.__enter__.return_value
            yield instance

    @pytest.fixture
    def sub_client(self) -> Generator[MagicMock]:
        with patch(f"{PUBSUB_CLIENT_MODULE_PATH}.SubscriberClient") as sub_client:
            sub_client.subscription_path.return_value = "some_sub_path"
            sub_client.topic_path.return_value = "some_topic_path"

            instance = sub_client.return_value.__enter__.return_value
            yield instance

    @pytest.mark.asyncio
    async def test_create_topic(self, pub_client: MagicMock, sub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        await client.create_topic("test-topic")

        pub_client.create_topic.assert_called_once()
        sub_client.create_subscription.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish(self, pub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        await client.publish("test-topic", data=b"test-data", ordering_key=None, attributes=None)

        pub_client.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_failure(self, pub_client: MagicMock):
        result = Future()
        result.set_exception(ValueError)
        pub_client.publish.return_value = result

        client = PubSubClient(project_id="test-project")
        with pytest.raises(ValueError):
            await client.publish(
                "test-topic", data=b"test-data", ordering_key=None, attributes=None
            )

    @pytest.mark.asyncio
    async def test_create_subscription(self, subscriber: Subscriber, sub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        await client.create_subscription(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
        )
        sub_client.create_subscription.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_subscription(self, subscriber: Subscriber, sub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        client.is_emulator = False
        await client.update_subscription(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
        )
        sub_client.update_subscription.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_subscription_not_found(
        self, subscriber: Subscriber, sub_client: MagicMock
    ):
        from google.api_core.exceptions import NotFound

        sub_client.update_subscription.side_effect = NotFound("test")
        client = PubSubClient(project_id="test-project")
        with pytest.raises(FastPubSubException):
            await client.update_subscription(
                topic_name=subscriber.topic_name,
                subscription_name=subscriber.subscription_name,
                retry_policy=subscriber.retry_policy,
                delivery_policy=subscriber.delivery_policy,
            )


# TODO:
# Test pulling
# Test ack/nack
# Test task manager and consumer task


"""
class TestCallbackHandler:
    @pytest.fixture
    def mock_asyncio(self) -> Generator[MagicMock]:
        with patch("fastpubsub.clients.sub.asyncio") as mock:
            yield mock

    def test_handle_success(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.ack_with_response.assert_called_once()

    def test_handle_drop(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        mock_asyncio.run.side_effect = Drop
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.ack_with_response.assert_called_once()

    def test_handle_retry(self, mock_asyncio: MagicMock, subscriber: Subscriber):
        mock_asyncio.run.side_effect = Retry
        handler = CallbackHandler(subscriber)
        message = MagicMock()
        message.attributes = {}

        handler.handle(message)

        mock_asyncio.run.assert_called_once()
        message.nack_with_response.assert_called_once()

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
"""
