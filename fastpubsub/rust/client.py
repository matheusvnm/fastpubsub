"""A Pub/Sub client that uses the Rust core."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar

from fastpubsub_core import (
    AdminClient,
    AlreadyExistsError,
    NotFoundError,
    Publisher,
)

from fastpubsub.datastructures import (
    DeadLetterPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import FastPubSubException

if TYPE_CHECKING:
    from fastpubsub_core import SubscriptionSettings

NotFound = NotFoundError
MAX_BATCH_SIZE = 256

logger = logging.getLogger(__name__)


def read_endpoint() -> str | None:
    """Read the emulator endpoint.

    Returns:
        The emulator URL, or None for the Pub/Sub service.
    """
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not emulator_host:
        return None
    return f"http://{emulator_host}"


def build_topic_path(project_id: str, topic_name: str) -> str:
    """Build the full path of a topic."""
    return f"projects/{project_id}/topics/{topic_name}"


def build_subscription_path(project_id: str, subscription_name: str) -> str:
    """Build the full path of a subscription."""
    return f"projects/{project_id}/subscriptions/{subscription_name}"


def resolve_futures(
    outcomes: list[tuple[asyncio.Future[str], bool, Any]],
) -> None:
    """Complete a batch of publish futures on the event loop.

    Args:
        outcomes: For each publish: the future, True when the publish
            succeeded, and the message id or the exception.
    """
    for future, is_success, value in outcomes:
        if future.done():
            continue
        if is_success:
            future.set_result(value)
        else:
            future.set_exception(value)


class PubSubClientFactory:
    """Cache the Rust admin client and the Rust publishers."""

    _admin_client: ClassVar[AdminClient | None] = None
    _publishers: ClassVar[
        dict[tuple[asyncio.AbstractEventLoop, str], Publisher]
    ] = {}

    @classmethod
    async def get_admin_client(cls) -> AdminClient:
        """Get or create the admin client."""
        if cls._admin_client is None:
            cls._admin_client = await asyncio.to_thread(
                AdminClient, read_endpoint()
            )
        return cls._admin_client

    @classmethod
    async def get_publisher(cls, topic_path: str) -> Publisher:
        """Get or create the publisher of a topic for the running loop."""
        loop = asyncio.get_running_loop()
        key = (loop, topic_path)
        publisher = cls._publishers.get(key)
        if publisher is None:
            publisher = await asyncio.to_thread(
                Publisher,
                topic_path,
                MAX_BATCH_SIZE,
                loop.call_soon_threadsafe,
                resolve_futures,
                read_endpoint(),
            )
            cls._publishers[key] = publisher
        return publisher

    @classmethod
    async def close_all(cls) -> None:
        """Send the pending messages and clear the cache."""
        publishers = list(cls._publishers.values())
        cls._publishers.clear()
        cls._admin_client = None
        for publisher in publishers:
            await asyncio.to_thread(publisher.flush)


class PubSubClient:
    """A Pub/Sub client that uses the Rust core."""

    def __init__(self, project_id: str) -> None:
        """Initializes the PubSubClient.

        Args:
            project_id: The Google Cloud project ID.
        """
        self.project_id = project_id
        self.is_emulator = bool(os.getenv("PUBSUB_EMULATOR_HOST"))

    def _build_settings(
        self,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None,
    ) -> SubscriptionSettings:
        dead_letter_topic = ""
        max_delivery_attempts = 0
        if dead_letter_policy:
            dead_letter_topic = build_topic_path(
                self.project_id, dead_letter_policy.topic_name
            )
            max_delivery_attempts = dead_letter_policy.max_delivery_attempts
        return {
            "filter": delivery_policy.filter_expression,
            "ack_deadline_seconds": delivery_policy.ack_deadline_seconds,
            "enable_message_ordering": delivery_policy.enable_message_ordering,
            "enable_exactly_once_delivery": (
                delivery_policy.enable_exactly_once_delivery
            ),
            "min_backoff_seconds": retry_policy.min_backoff_delay_secs,
            "max_backoff_seconds": retry_policy.max_backoff_delay_secs,
            "dead_letter_topic": dead_letter_topic,
            "max_delivery_attempts": max_delivery_attempts,
        }

    async def create_topic(
        self, topic_name: str, create_default_subscription: bool = True
    ) -> None:
        """Creates a topic.

        Args:
            topic_name: The name of the topic.
            create_default_subscription: Whether to create a default
                subscription for the topic.
        """
        admin_client = await PubSubClientFactory.get_admin_client()
        topic_path = build_topic_path(self.project_id, topic_name)
        with suppress(AlreadyExistsError):
            await asyncio.to_thread(admin_client.create_topic, topic_path)
            logger.debug(f"Created topic '{topic_path}' successfully.")
            if not create_default_subscription:
                return
            await asyncio.to_thread(
                admin_client.create_subscription,
                build_subscription_path(self.project_id, topic_name),
                topic_path,
                {},
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
        admin_client = await PubSubClientFactory.get_admin_client()
        with suppress(AlreadyExistsError):
            await asyncio.to_thread(
                admin_client.create_subscription,
                build_subscription_path(self.project_id, subscription_name),
                build_topic_path(self.project_id, topic_name),
                self._build_settings(
                    retry_policy, delivery_policy, dead_letter_policy
                ),
            )

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
        update_fields = [
            "ack_deadline_seconds",
            "dead_letter_policy",
            "retry_policy",
            "enable_exactly_once_delivery",
        ]
        if not self.is_emulator:
            update_fields.append("filter")

        admin_client = await PubSubClientFactory.get_admin_client()
        subscription_path = build_subscription_path(
            self.project_id, subscription_name
        )
        topic_path = build_topic_path(self.project_id, topic_name)
        try:
            await asyncio.to_thread(
                admin_client.update_subscription,
                subscription_path,
                topic_path,
                self._build_settings(
                    retry_policy, delivery_policy, dead_letter_policy
                ),
                update_fields,
            )
        except NotFoundError as e:
            raise FastPubSubException(
                "We could not update the subscription configuration. "
                f"The topic {topic_path} or "
                f"subscription {subscription_path} were not found. "
                "They may be deleted or not autocreated. "
                "Please, setup your @subscriber with the 'autocreate=True' "
                "option to automatically create them."
            ) from e

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
        topic_path = build_topic_path(self.project_id, topic_name)
        publisher = await PubSubClientFactory.get_publisher(topic_path)
        future: asyncio.Future[str] = (
            asyncio.get_running_loop().create_future()
        )
        try:
            publisher.publish(data, attributes or {}, future, ordering_key)
            message_id = await future
            logger.info(
                f"Message published for topic {topic_path} "
                f"with id {message_id}"
            )
        except Exception:
            logger.exception("Publisher failure", stacklevel=5)
            raise
