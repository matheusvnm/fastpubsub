"""A client for interacting with Google Cloud Pub/Sub."""

import logging
import os
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import suppress
from datetime import timedelta
from typing import Any

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.types import FlowControl, PublisherOptions
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub import DeadLetterPolicy as DLTPolicy
from google.pubsub import RetryPolicy, Subscription

from fastpubsub.clients.scheduler import AsyncScheduler
from fastpubsub.concurrency.utils import apply_async
from fastpubsub.datastructures import DeadLetterPolicy, MessageDeliveryPolicy, MessageRetryPolicy
from fastpubsub.exceptions import FastPubSubException

DEFAULT_PUBSUB_TIMEOUT = 20.0
DEFAULT_PULL_TIMEOUT = 120.0
DEFAULT_PUSH_TIMEOUT = 60.0

logger = logging.getLogger(__name__)


class PubSubClient:
    """A client for interacting with Google Cloud Pub/Sub."""

    def __init__(self, project_id: str) -> None:
        """Initializes the PubSubClient.

        Args:
            project_id: The Google Cloud project ID.
        """
        self.project_id = project_id
        self.is_emulator = True if os.getenv("PUBSUB_EMULATOR_HOST") else False
        self.subscriber_client: SubscriberClient = SubscriberClient()

    def __del__(self) -> None:
        """Frees the objects used in PubSubClient."""
        if self.subscriber_client and not self.subscriber_client.closed:
            self.subscriber_client.transport.close()

    def _create_subscription_request(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> Subscription:
        name = SubscriberClient.subscription_path(self.project_id, subscription_name)
        topic = SubscriberClient.topic_path(self.project_id, topic_name)

        dlt_policy = None
        if dead_letter_policy:
            dlt_topic = SubscriberClient.topic_path(
                self.project_id,
                dead_letter_policy.topic_name,
            )

            dlt_policy = DLTPolicy(
                dead_letter_topic=dlt_topic,
                max_delivery_attempts=dead_letter_policy.max_delivery_attempts,
            )

        min_backoff_delay = timedelta(seconds=retry_policy.min_backoff_delay_secs)
        max_backoff_delay = timedelta(seconds=retry_policy.max_backoff_delay_secs)
        message_retry_policy = RetryPolicy(
            minimum_backoff=min_backoff_delay, maximum_backoff=max_backoff_delay
        )

        return Subscription(
            name=name,
            topic=topic,
            dead_letter_policy=dlt_policy,
            retry_policy=message_retry_policy,
            filter=delivery_policy.filter_expression,
            ack_deadline_seconds=delivery_policy.ack_deadline_seconds,
            enable_exactly_once_delivery=delivery_policy.enable_exactly_once_delivery,
        )

    async def create_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> None:
        """Creates a subscription.

        Args:
            topic_name: The name of the topic.
            subscription_name: The name of the subscription.
            retry_policy: The retry policy for the subscription.
            delivery_policy: The delivery policy for the subscription.
            dead_letter_policy: The dead-letter policy for the subscription.
        """
        subscription_request = self._create_subscription_request(
            topic_name=topic_name,
            subscription_name=subscription_name,
            retry_policy=retry_policy,
            delivery_policy=delivery_policy,
            dead_letter_policy=dead_letter_policy,
        )

        with suppress(AlreadyExists):
            logger.debug(f"Attempting to create subscription: {subscription_request.name}")
            await apply_async(
                self.subscriber_client.create_subscription,
                request=subscription_request,
                timeout=DEFAULT_PUBSUB_TIMEOUT,
            )

            logger.debug(f"Successfully created subscription: {subscription_request.name}")

    async def update_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> None:
        """Updates a subscription.

        Args:
            topic_name: The name of the topic.
            subscription_name: The name of the subscription.
            retry_policy: The retry policy for the subscription.
            delivery_policy: The delivery policy for the subscription.
            dead_letter_policy: The dead-letter policy for the subscription.
        """
        subscription_request = self._create_subscription_request(
            topic_name=topic_name,
            subscription_name=subscription_name,
            retry_policy=retry_policy,
            delivery_policy=delivery_policy,
            dead_letter_policy=dead_letter_policy,
        )

        update_fields = [
            "ack_deadline_seconds",
            "dead_letter_policy",
            "retry_policy",
            "enable_exactly_once_delivery",
        ]

        if not self.is_emulator:
            update_fields.append("filter")

        update_mask = FieldMask(paths=update_fields)

        try:
            logger.debug(f"Attempting to update the subscription: {subscription_request.name}")
            response = await apply_async(
                self.subscriber_client.update_subscription,
                subscription=subscription_request,
                update_mask=update_mask,
                timeout=DEFAULT_PUBSUB_TIMEOUT,
            )

            logger.debug(f"Successfully updated the subscription: {subscription_request.name}")
            logger.debug(f"The subscription is now following the configuration: {response}")
        except NotFound as e:
            raise FastPubSubException(
                "We could not update the subscription configuration. "
                f"The topic {subscription_request.topic} or "
                f"subscription {subscription_request.name} were not found. "
                "They may be deleted or not autocreated. "
                "Please, setup your @subscriber with the 'autocreate=True' "
                "option to automatically create them."
            ) from e

    async def create_topic(self, topic_name: str, create_default_subscription: bool = True) -> None:
        """Creates a topic.

        Args:
            topic_name: The name of the topic.
            create_default_subscription: Whether to create a default
                subscription for the topic.
        """
        publisher = PublisherClient()
        with suppress(AlreadyExists):
            logger.debug(f"Creating topic '{topic_name}'.")
            topic_path = PublisherClient.topic_path(self.project_id, topic_name)

            topic = await apply_async(publisher.create_topic, name=topic_path)
            logger.debug(f"Created topic '{topic.name}' successfully.")

            if not create_default_subscription:
                return

            logger.debug(f"Creating default subscription for '{topic_path}'.")
            default_subscription_path = SubscriberClient.subscription_path(
                self.project_id, topic_name
            )
            subscription = await apply_async(
                self.subscriber_client.create_subscription,
                name=default_subscription_path,
                topic=topic_path,
                timeout=DEFAULT_PULL_TIMEOUT,
            )

            logger.debug(
                "Creating default subscription created successfully for "
                f"'{topic_path}' as {subscription.name}."
            )

    async def publish(
        self,
        topic_name: str,
        *,
        data: bytes,
        ordering_key: str,
        attributes: dict[str, str] | None,
    ) -> None:
        """Publishes a message.

        Args:
            topic_name: The name of the topic.
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
        """
        topic_path = PublisherClient.topic_path(self.project_id, topic_name)
        new_attributes = {} if attributes is None else attributes

        try:
            publisher_options = PublisherOptions(enable_message_ordering=bool(ordering_key))
            publisher = PublisherClient(publisher_options=publisher_options)
            response: Future[str] = publisher.publish(
                topic=topic_path,
                data=data,
                ordering_key=ordering_key,
                timeout=DEFAULT_PUSH_TIMEOUT,
                **new_attributes,
            )

            message_id = await apply_async(response.result)
            logger.info(f"Message published for topic {topic_path} with id {message_id}")
            logger.debug(f"We sent {data!r} with metadata {attributes}")
        except Exception:
            logger.exception("Publisher failure", stacklevel=5)
            raise

    def subscribe(
        self,
        callback: Callable[[PubSubMessage], Any],
        subscription_name: str,
        max_messages: int,
    ) -> StreamingPullFuture:
        """Starts the subscription listening on background given a subscription.

        Args:
            callback: The function called when a message is received.
            subscription_name: The name of the subscription.
            max_messages: The maximum number of messages to pull.

        Returns:
            A future that can be used to check the progress and get the result.
        """
        subscription_path = SubscriberClient.subscription_path(self.project_id, subscription_name)
        future: StreamingPullFuture = self.subscriber_client.subscribe(
            callback=callback,
            subscription=subscription_path,
            scheduler=AsyncScheduler(),
            flow_control=FlowControl(max_messages=max_messages),
            await_callbacks_on_shutdown=True,
        )
        return future
