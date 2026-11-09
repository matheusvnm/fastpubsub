"""The Pub/Sub clients of the library, built on the compiled core."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, ClassVar

from fastpubsub_core import AdminClient, AlreadyExistsError, Publisher
from fastpubsub_core import NotFoundError as CoreNotFoundError

from fastpubsub._internal.connection import Connection
from fastpubsub._internal.event_loop_bridge import EventLoopBridge
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import FastPubSubException, NotFoundError

if TYPE_CHECKING:
    from fastpubsub_core import SubscriptionSettings

logger = logging.getLogger(__name__)


class PubSubClientFactory:
    """Caches the admin clients and the publishers of each event loop.

    The cache keeps the creation task of each client, so concurrent first
    calls for the same key wait for the same task and get the same client.

    The class needs no lock on free-threaded Python. Each key holds an
    event loop, so only the thread of that loop creates or reads the
    entry, and each single ``dict`` operation is atomic.
    """

    _admin_clients: ClassVar[
        dict[asyncio.AbstractEventLoop, asyncio.Task[AdminClient]]
    ] = {}
    _publisher_creations: ClassVar[
        dict[tuple[asyncio.AbstractEventLoop, str], asyncio.Task[Publisher]]
    ] = {}
    _publishers: ClassVar[
        dict[tuple[asyncio.AbstractEventLoop, str], Publisher]
    ] = {}

    @classmethod
    async def get_admin_client(cls) -> AdminClient:
        """Gets or creates the admin client of the running event loop.

        Returns:
            The admin client.
        """
        loop = asyncio.get_running_loop()
        creation = cls._admin_clients.get(loop)
        if creation is None:
            creation = loop.create_task(
                asyncio.to_thread(AdminClient, Connection.read_endpoint())
            )
            cls._admin_clients[loop] = creation
        return await asyncio.shield(creation)

    @classmethod
    async def get_publisher(cls, topic_path: str) -> Publisher:
        """Gets or creates the publisher of a topic for the running loop.

        Args:
            topic_path: The full path of the topic.

        Returns:
            The publisher of the topic.
        """
        loop = asyncio.get_running_loop()
        key = (loop, topic_path)
        publisher = cls._publishers.get(key)
        if publisher is not None:
            return publisher

        creation = cls._publisher_creations.get(key)
        if creation is None:
            creation = loop.create_task(
                asyncio.to_thread(
                    Publisher,
                    topic_path,
                    EventLoopBridge.get(),
                    EventLoopBridge.resolve_futures,
                    Connection.read_endpoint(),
                )
            )
            cls._publisher_creations[key] = creation
        publisher = await asyncio.shield(creation)
        cls._publishers[key] = publisher
        return publisher

    @classmethod
    async def close_all(cls) -> None:
        """Sends the pending messages and clears the cache."""
        creations = list(cls._publisher_creations.values())
        cls._publisher_creations.clear()
        cls._publishers.clear()
        cls._admin_clients.clear()
        EventLoopBridge.clear()
        for creation in creations:
            with suppress(Exception):
                publisher = await creation
                await asyncio.to_thread(publisher.flush)


class PubSubClient:
    """Creates topics and subscriptions, and publishes messages."""

    def __init__(self, project_id: str) -> None:
        """Initializes the PubSubClient.

        Args:
            project_id: The Google Cloud project ID.
        """
        self.project_id = project_id
        self.connection = Connection(project_id)
        self.is_emulator = Connection.is_emulator()

    def _build_settings(
        self,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None,
    ) -> SubscriptionSettings:
        """Converts the subscriber policies to the core subscription settings.

        Args:
            retry_policy: The retry policy.
            delivery_policy: The delivery policy.
            dead_letter_policy: The dead-letter policy, or None.

        Returns:
            The settings for ``AdminClient.create_subscription`` and
            ``AdminClient.update_subscription``.
        """
        dead_letter_topic = ""
        max_delivery_attempts = 0
        if dead_letter_policy:
            dead_letter_topic = self.connection.topic_path(
                dead_letter_policy.topic_name
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
        topic_path = self.connection.topic_path(topic_name)
        with suppress(AlreadyExistsError):
            await asyncio.to_thread(admin_client.create_topic, topic_path)
            logger.debug(f"Created topic '{topic_path}' successfully.")
            if not create_default_subscription:
                return
            await asyncio.to_thread(
                admin_client.create_subscription,
                self.connection.subscription_path(topic_name),
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
                self.connection.subscription_path(subscription_name),
                self.connection.topic_path(topic_name),
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
        subscription_path = self.connection.subscription_path(
            subscription_name
        )
        topic_path = self.connection.topic_path(topic_name)
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
        except CoreNotFoundError as e:
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

        Raises:
            NotFoundError: The topic does not exist.
        """
        topic_path = self.connection.topic_path(topic_name)
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
        except CoreNotFoundError as e:
            logger.exception("Publisher failure", stacklevel=5)
            raise NotFoundError(str(e)) from e
        except Exception:
            logger.exception("Publisher failure", stacklevel=5)
            raise
