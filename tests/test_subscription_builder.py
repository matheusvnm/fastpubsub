from collections.abc import Generator
from copy import deepcopy
from unittest.mock import MagicMock, call, patch

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

BUILDER_MODULE_PATH = "fastpubsub.clients.builder"


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
            enable_message_ordering=False,
            enable_exactly_once_delivery=False,
        )

        lifecycle_policy = LifecyclePolicy(autocreate=True, autoupdate=True)

        control_flow_policy = MessageControlFlowPolicy(
            max_messages=100,
            max_bytes=1024,
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

        subscriber.set_project_id("proj_id")
        return subscriber

    @pytest.fixture
    def sub_client(self) -> Generator[MagicMock]:
        with patch(f"{BUILDER_MODULE_PATH}.PubSubSubscriberClient") as sub_client:
            yield sub_client

    @pytest.fixture
    def pub_client(self) -> Generator[MagicMock]:
        with patch(f"{BUILDER_MODULE_PATH}.PubSubPublisherClient") as pub_client:
            yield pub_client

    def test_build_full_subscription_successfully(
        self, sub_client: MagicMock, pub_client: MagicMock, subscriber: Subscriber
    ):
        subscription_builder = PubSubSubscriptionBuilder()
        subscription_builder.build(subscriber=subscriber)

        expected_calls = [
            call(project_id="proj_id", topic_name="topic"),
            call().create_topic(False),
            call(project_id="proj_id", topic_name="dlt_topic"),
            call().create_topic(True),
        ]
        assert pub_client.mock_calls == expected_calls

        sub_client_instance = sub_client.return_value
        sub_client_instance.create_subscription.assert_called_once_with(subscriber=subscriber)
        sub_client_instance.update_subscription.assert_called_once_with(subscriber=subscriber)

    def test_build_subscription_no_autoupdate(
        self, sub_client: MagicMock, pub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.lifecycle_policy = LifecyclePolicy(autocreate=True, autoupdate=False)
        subscription_builder = PubSubSubscriptionBuilder()
        subscription_builder.build(subscriber=subscriber)

        expected_calls = [
            call(project_id="proj_id", topic_name="topic"),
            call().create_topic(False),
            call(project_id="proj_id", topic_name="dlt_topic"),
            call().create_topic(True),
        ]
        assert pub_client.mock_calls == expected_calls

        sub_client_instance = sub_client.return_value
        sub_client_instance.create_subscription.assert_called_once_with(subscriber=subscriber)
        sub_client_instance.update_subscription.assert_not_called()

    def test_build_subscription_no_autocreate(
        self, sub_client: MagicMock, pub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.lifecycle_policy = LifecyclePolicy(autocreate=False, autoupdate=True)
        subscription_builder = PubSubSubscriptionBuilder()
        subscription_builder.build(subscriber=subscriber)

        pub_client.assert_not_called()
        sub_client_instance = sub_client.return_value
        sub_client_instance.create_subscription.assert_not_called()
        sub_client_instance.update_subscription.assert_called_once_with(subscriber=subscriber)

    def test_build_subscription_topic_no_dlt(
        self, sub_client: MagicMock, pub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber.dead_letter_policy = None
        subscription_builder = PubSubSubscriptionBuilder()
        subscription_builder.build(subscriber=subscriber)

        expected_calls = [
            call(project_id="proj_id", topic_name="topic"),
            call().create_topic(False),
        ]
        assert pub_client.mock_calls == expected_calls

        sub_client_instance = sub_client.return_value
        sub_client_instance.create_subscription.assert_called_once_with(subscriber=subscriber)
        sub_client_instance.update_subscription.assert_called_once_with(subscriber=subscriber)

    def test_topic_only_created_once(
        self, sub_client: MagicMock, pub_client: MagicMock, subscriber: Subscriber
    ):
        subscriber_one = deepcopy(subscriber)
        subscriber_two = deepcopy(subscriber)
        subscription_builder = PubSubSubscriptionBuilder()
        subscription_builder.build(subscriber=subscriber_one)
        subscription_builder.build(subscriber=subscriber_two)

        expected_calls = [
            call(project_id="proj_id", topic_name="topic"),
            call().create_topic(False),
            call(project_id="proj_id", topic_name="dlt_topic"),
            call().create_topic(True),
        ]
        assert pub_client.mock_calls == expected_calls

        expected_calls = [
            call(),
            call().create_subscription(subscriber=subscriber_one),
            call(),
            call().update_subscription(subscriber=subscriber_one),
            call(),
            call().create_subscription(subscriber=subscriber_two),
            call(),
            call().update_subscription(subscriber=subscriber_two),
        ]
        assert sub_client.mock_calls == expected_calls
