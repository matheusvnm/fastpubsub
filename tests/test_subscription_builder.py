from collections.abc import Generator
from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from fastpubsub import Subscriber
from fastpubsub.builder import PubSubSubscriptionBuilder
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)

BUILDER_MODULE_PATH = "fastpubsub.builder"


class TestSubscriptionBuilder:
    @pytest.fixture
    def subscriber(self):
        dead_letter_policy = DeadLetterPolicy(topic_name="dlt_topic", max_delivery_attempts=5)

        retry_policy = MessageRetryPolicy(
            min_backoff_delay_secs=10,
            max_backoff_delay_secs=600,
        )

        delivery_policy = MessageDeliveryPolicy(
            filter_expression="",
            ack_deadline_seconds=60,
            enable_exactly_once_delivery=False,
        )

        lifecycle_policy = LifecyclePolicy(autocreate=True, autoupdate=True)

        control_flow_policy = MessageControlFlowPolicy(
            max_messages=100,
        )

        subscriber = Subscriber(
            func=lambda: "",
            topic_name="topic",
            subscription_name="sub",
            retry_policy=retry_policy,
            delivery_policy=delivery_policy,
            lifecycle_policy=lifecycle_policy,
            control_flow_policy=control_flow_policy,
            dead_letter_policy=dead_letter_policy,
            middlewares=[],
        )

        subscriber._set_project_id("proj_id")
        return subscriber

    @pytest.fixture
    def pubsub_client(self) -> Generator[MagicMock]:
        with patch(f"{BUILDER_MODULE_PATH}.PubSubClient") as pubsub_client:
            instance = pubsub_client.return_value
            instance.create_topic = AsyncMock()
            instance.create_subscription = AsyncMock()
            instance.update_subscription = AsyncMock()
            yield instance

    @pytest.mark.asyncio
    async def test_build_full_subscription_successfully(
        self, pubsub_client: MagicMock, subscriber: Subscriber
    ):
        subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
        await subscription_builder.build(subscriber=subscriber)

        expected_calls = [
            call.create_topic(topic_name=subscriber.topic_name, create_default_subscription=False),
            call.create_topic(
                topic_name=subscriber.dead_letter_policy.topic_name,
                create_default_subscription=True,
            ),
            call.create_subscription(
                topic_name=subscriber.topic_name,
                subscription_name=subscriber.subscription_name,
                retry_policy=subscriber.retry_policy,
                delivery_policy=subscriber.delivery_policy,
                dead_letter_policy=subscriber.dead_letter_policy,
            ),
            call.update_subscription(
                topic_name=subscriber.topic_name,
                subscription_name=subscriber.subscription_name,
                retry_policy=subscriber.retry_policy,
                delivery_policy=subscriber.delivery_policy,
                dead_letter_policy=subscriber.dead_letter_policy,
            ),
        ]

        pubsub_client.assert_has_calls(expected_calls, any_order=True)

    @pytest.mark.asyncio
    async def test_build_subscription_no_autoupdate(
        self, pubsub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.lifecycle_policy = LifecyclePolicy(autocreate=True, autoupdate=False)
        subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
        await subscription_builder.build(subscriber=subscriber)

        assert pubsub_client.create_topic.call_count == 2
        pubsub_client.create_topic.assert_called_with(
            topic_name=subscriber.dead_letter_policy.topic_name, create_default_subscription=True
        )

        pubsub_client.create_subscription.assert_called_once_with(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
            dead_letter_policy=subscriber.dead_letter_policy,
        )

        pubsub_client.update_subscription.assert_not_called()

    @pytest.mark.asyncio
    async def test_build_subscription_no_autocreate(
        self, pubsub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.lifecycle_policy = LifecyclePolicy(autocreate=False, autoupdate=True)
        subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
        await subscription_builder.build(subscriber=subscriber)

        pubsub_client.create_topic.assert_not_called()
        pubsub_client.create_subscription.assert_not_called()
        pubsub_client.update_subscription.assert_called_once_with(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
            dead_letter_policy=subscriber.dead_letter_policy,
        )

    @pytest.mark.asyncio
    async def test_build_subscription_topic_no_dlt(
        self, pubsub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.dead_letter_policy = None
        subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
        await subscription_builder.build(subscriber=subscriber)

        pubsub_client.create_topic.assert_called_once_with(
            topic_name=subscriber.topic_name, create_default_subscription=False
        )

        pubsub_client.create_subscription.assert_called_once_with(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
            dead_letter_policy=subscriber.dead_letter_policy,
        )

        pubsub_client.update_subscription.assert_called_once_with(
            topic_name=subscriber.topic_name,
            subscription_name=subscriber.subscription_name,
            retry_policy=subscriber.retry_policy,
            delivery_policy=subscriber.delivery_policy,
            dead_letter_policy=subscriber.dead_letter_policy,
        )

    @pytest.mark.asyncio
    async def test_topic_only_created_once(self, pubsub_client: MagicMock, subscriber: Subscriber):
        subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
        subscriber_one = deepcopy(subscriber)
        await subscription_builder.build(subscriber=subscriber_one)

        expected_calls = [
            call.create_topic(
                topic_name=subscriber_one.topic_name, create_default_subscription=False
            ),
            call.create_topic(
                topic_name=subscriber_one.dead_letter_policy.topic_name,
                create_default_subscription=True,
            ),
            call.create_subscription(
                topic_name=subscriber_one.topic_name,
                subscription_name=subscriber_one.subscription_name,
                retry_policy=subscriber_one.retry_policy,
                delivery_policy=subscriber_one.delivery_policy,
                dead_letter_policy=subscriber_one.dead_letter_policy,
            ),
            call.update_subscription(
                topic_name=subscriber_one.topic_name,
                subscription_name=subscriber_one.subscription_name,
                retry_policy=subscriber_one.retry_policy,
                delivery_policy=subscriber_one.delivery_policy,
                dead_letter_policy=subscriber_one.dead_letter_policy,
            ),
        ]

        assert pubsub_client.create_topic.call_count == 2
        pubsub_client.assert_has_calls(expected_calls, any_order=True)

        subscriber_two = deepcopy(subscriber)
        await subscription_builder.build(subscriber=subscriber_two)
        assert pubsub_client.create_topic.call_count == 2
