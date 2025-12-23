"""Broker implementation."""

import logging
import os
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub.builder import PubSubSubscriptionBuilder
from fastpubsub.concurrency.manager import AsyncTaskManager
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter
from fastpubsub.types import SubscribedCallable

logger = logging.getLogger(__name__)


class PubSubBroker:
    """Manages the connection with PubSub Broker."""

    def __init__(
        self,
        project_id: str,
        routers: Sequence[PubSubRouter] | None = None,
        middlewares: Sequence[type[BaseMiddleware]] | None = None,
    ):
        """Initializes the PubSubBroker.

        Args:
            project_id: The Google Cloud project ID.
            routers: A sequence of routers to include.
            middlewares: A sequence of middlewares to apply to all messages
                incoming to subscribers and publishers.
        """
        if not (project_id and isinstance(project_id, str) and len(project_id.strip()) > 0):
            raise FastPubSubException(f"The project id value ({project_id}) is invalid.")

        self.project_id = project_id
        self.router = PubSubRouter(routers=routers, project_id=project_id, middlewares=middlewares)
        self.task_manager = AsyncTaskManager()

    @validate_call(config=ConfigDict(strict=True))
    def subscriber(
        self,
        alias: str,
        *,
        topic_name: str,
        subscription_name: str,
        project_id: str = "",
        autocreate: bool = True,
        autoupdate: bool = False,
        filter_expression: str = "",
        dead_letter_topic: str = "",
        max_delivery_attempts: int = 5,
        ack_deadline_seconds: int = 60,
        enable_message_ordering: bool = False,
        enable_exactly_once_delivery: bool = False,
        min_backoff_delay_secs: int = 10,
        max_backoff_delay_secs: int = 600,
        max_messages: int = 1000,
        middlewares: Sequence[type[BaseMiddleware]] | None = None,
    ) -> SubscribedCallable:
        """Decorator to register a function as a subscriber.

        Args:
            alias: A unique name for the subscriber. You can use this alias to
                select which subscription to use on the CLI.
            topic_name: The name of the topic to subscribe to.
            subscription_name: The name of the subscription attached to the topic.
            project_id: An alternative project id to create a subscription
                and consume messages from. If set the broker project id
                will be ignored.
            autocreate: Whether to automatically create the topic and
                subscription if they do not exists.
            autoupdate: Whether to automatically update the subscription.
            filter_expression: A filter expression to apply to the
                subscription to filter messages.
            dead_letter_topic: The name of the dead-letter topic.
            max_delivery_attempts: The maximum number of delivery attempts
                before sending the message to the dead-letter.
            ack_deadline_seconds: The acknowledgment deadline in seconds.
            enable_message_ordering: Whether the message must be delivered in order.
            enable_exactly_once_delivery: Whether to enable exactly-once delivery.
            min_backoff_delay_secs: The minimum backoff delay in seconds.
            max_backoff_delay_secs: The maximum backoff delay in seconds.
            max_messages: The maximum number of messages to fetch from the broker.
            middlewares: A sequence of middlewares to apply **only to the subscriber**.

        Returns:
            A decorator that registers the function as a subscriber.
        """
        return self.router.subscriber(
            alias=alias,
            topic_name=topic_name,
            subscription_name=subscription_name,
            project_id=project_id,
            autocreate=autocreate,
            autoupdate=autoupdate,
            filter_expression=filter_expression,
            dead_letter_topic=dead_letter_topic,
            max_delivery_attempts=max_delivery_attempts,
            ack_deadline_seconds=ack_deadline_seconds,
            enable_message_ordering=enable_message_ordering,
            enable_exactly_once_delivery=enable_exactly_once_delivery,
            min_backoff_delay_secs=min_backoff_delay_secs,
            max_backoff_delay_secs=max_backoff_delay_secs,
            max_messages=max_messages,
            middlewares=middlewares,
        )

    @validate_call(config=ConfigDict(strict=True))
    def publisher(self, topic_name: str, project_id: str = "") -> Publisher:
        """Returns a publisher for the given topic.

        Args:
            topic_name: The name of the topic.
            project_id: An alternative project id to publish messages.
                        If set the broker's project id will be ignored.

        Returns:
            A publisher for the given topic.
        """
        return self.router.publisher(topic_name=topic_name, project_id=project_id)

    @validate_call(config=ConfigDict(strict=True))
    async def publish(
        self,
        topic_name: str,
        data: dict[str, Any] | str | bytes | BaseModel,
        project_id: str = "",
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        """Publishes a message to the given topic.

        Args:
            topic_name: The name of the topic.
            data: The message data.
            project_id: An alternative project id to publish messages.
                        If set the broker's project id will be ignored.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
            autocreate: Whether to automatically create the topic if it does not exists.
        """
        return await self.router.publish(
            topic_name=topic_name,
            data=data,
            project_id=project_id,
            ordering_key=ordering_key,
            attributes=attributes,
            autocreate=autocreate,
        )

    def include_router(self, router: PubSubRouter) -> None:
        """Includes a router in the broker.

        Args:
            router: The router to include.
        """
        return self.router.include_router(router)

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        """Includes a middleware in the broker.

        Args:
            middleware: The middleware to include.
        """
        return self.router.include_middleware(middleware)

    async def start(self) -> None:
        """Starts the broker."""
        subscribers = self._filter_subscribers()
        if not subscribers:
            logger.error("No subscriber found for running.")
            raise FastPubSubException(
                "You must select the subscribers using --subscribers flag or run them all."
            )

        for subscriber in subscribers:
            subscription_builder = PubSubSubscriptionBuilder(project_id=subscriber.project_id)
            await subscription_builder.build(subscriber)
            self.task_manager.create_task(subscriber)

        self.task_manager.start()

    def alive(self) -> bool:
        """Checks if the message consumer tasks are alive.

        Returns:
            True if they are alive, False otherwise.
        """
        subscribers = self.task_manager.alive()
        if not subscribers:
            logger.info("The subscribers are not active. May be they are deactivated?")
            return False

        for name, liveness in subscribers.items():
            if not liveness:
                logger.error(f"The {name} subscriber handler is not alive")
                return False

        return True

    def ready(self) -> bool:
        """Checks if the message consumer tasks are ready.

        Returns:
            True if they are ready, False otherwise.
        """
        subscribers = self.task_manager.ready()
        if not subscribers:
            logger.info("The subscribers are not active. May be they are deactivated?")
            return False

        for name, readiness in subscribers.items():
            if not readiness:
                logger.error(f"The {name} subscriber handler is not ready")
                return False

        return True

    def _filter_subscribers(self) -> list[Subscriber]:
        subscribers = self.router._get_subscribers()
        selected_subscribers = self._get_selected_subscribers()

        if not selected_subscribers:
            logger.debug(f"Running all the subscribers as {list(subscribers.keys())}")
            return list(subscribers.values())

        found_subscribers = []
        for selected_subscriber in selected_subscribers:
            if selected_subscriber not in subscribers:
                logger.warning(f"The '{selected_subscriber}' subscriber alias not found")
                continue

            logger.debug(f"We have found the subscriber '{selected_subscriber}'")
            found_subscribers.append(subscribers[selected_subscriber])

        return found_subscribers

    def _get_selected_subscribers(self) -> set[str]:
        selected_subscribers: set[str] = set()
        subscribers_text = os.getenv("FASTPUBSUB_SUBSCRIBERS", "")
        if not subscribers_text:
            return selected_subscribers

        dirty_aliases = subscribers_text.split(",")
        for dirty_alias in dirty_aliases:
            clean_alias = dirty_alias.lower().strip()
            if clean_alias:
                selected_subscribers.add(clean_alias)

        return selected_subscribers

    def shutdown(self) -> None:
        """Shuts down the broker."""
        self.task_manager.shutdown()
