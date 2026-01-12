from collections.abc import Generator
from concurrent.futures import Future
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fastpubsub.clients.pubsub import DEFAULT_PUSH_TIMEOUT, PubSubClient
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
FACTORY_MODULE_PATH = "fastpubsub.clients.factory.PubSubClientFactory"


@pytest.fixture
def subscriber():
    async def dummy_handler(msg):
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
    subscriber._set_project_id("test-project")
    subscriber._build_callstack = MagicMock()
    return subscriber


class TestPubSubClient:
    @pytest.fixture
    def pub_client(self) -> Generator[MagicMock]:
        mock_publisher = MagicMock()
        mock_publisher.topic_path = MagicMock(return_value="some_topic_path")
        mock_publisher.create_topic = MagicMock()
        mock_publisher.publish = MagicMock()

        with patch(
            f"{FACTORY_MODULE_PATH}.get_publisher",
            new_callable=AsyncMock,
            return_value=mock_publisher,
        ):
            yield mock_publisher

    @pytest.fixture
    def sub_client(self) -> Generator[MagicMock]:
        mock_subscriber = MagicMock()
        mock_subscriber.subscription_path = MagicMock(return_value="some_sub_path")
        mock_subscriber.topic_path = MagicMock(return_value="some_topic_path")
        mock_subscriber.create_subscription = MagicMock()
        mock_subscriber.update_subscription = MagicMock()

        with patch(
            f"{FACTORY_MODULE_PATH}.get_subscriber",
            new_callable=AsyncMock,
            return_value=mock_subscriber,
        ):
            yield mock_subscriber

    @pytest.mark.asyncio
    async def test_create_topic(self, pub_client: MagicMock, sub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        await client.create_topic("test-topic")

        pub_client.create_topic.assert_called_once()
        sub_client.create_subscription.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_topic_no_default_sub(self, pub_client: MagicMock, sub_client: MagicMock):
        client = PubSubClient(project_id="test-project")
        await client.create_topic("test-topic", False)

        pub_client.create_topic.assert_called_once()
        sub_client.create_subscription.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish(self, pub_client: MagicMock, sub_client: MagicMock):
        project_id = "some_proj"
        topic_name = "some_topic"
        topic_path = "some_topic_path"
        data = b"some_data"

        client = PubSubClient(project_id=project_id)
        await client.publish(topic_name, data=data, ordering_key="", attributes=None)

        pub_client.topic_path.assert_called_once_with(project_id, topic_name)
        pub_client.publish.assert_called_once_with(
            topic=topic_path, data=data, ordering_key="", timeout=DEFAULT_PUSH_TIMEOUT
        )

    @pytest.mark.asyncio
    async def test_publish_failure(self, pub_client: MagicMock, sub_client: MagicMock):
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
            dead_letter_policy=subscriber.dead_letter_policy,
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
