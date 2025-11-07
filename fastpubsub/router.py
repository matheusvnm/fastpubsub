"""A router for organizing publishers and subscribers."""

import re
from collections import OrderedDict
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub.concurrency.utils import ensure_async_callable_function
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.types import AsyncDecoratedCallable, SubscribedCallable

_PREFIX_REGEX = re.compile(r"^[a-zA-Z0-9]+([_./][a-zA-Z0-9]+)*$")


class PubSubRouter:
    """A router for organizing publishers and subscribers."""

    def __init__(
        self,
        prefix: str = "",
        *,
        routers: Sequence["PubSubRouter"] | None = None,
        middlewares: Sequence[type[BaseMiddleware]] | None = None,
    ):
        """Initializes the PubSubRouter.

        Args:
            prefix: A prefix to apply to all subscribers and publishers in the
                router. If set, the subscriber alias will be: <prefix>.<alias>.
                Also, it affects the subscription name. A subscription will be
                <prefix>.<subscription_name>.
            routers: A sequence of childrens routers to include.
            middlewares: A sequence of middlewares to apply to all subscribers
                in this router and its children.
        """
        if prefix and not _PREFIX_REGEX.match(prefix):
            raise FastPubSubException(
                "Prefix must be a string that starts and ends with a letter or number, "
                "and can only contain periods, slashes, or underscores in the middle."
            )

        self.prefix = prefix
        self.project_id: str = ""
        self.routers: list[PubSubRouter] = []
        self.publishers: dict[str, Publisher] = {}
        self.subscribers: dict[str, Subscriber] = {}
        self.middlewares: list[type[BaseMiddleware]] = []

        if routers:
            if not isinstance(routers, Sequence):
                raise FastPubSubException("Your routers should be passed as a sequence")

            for router in routers:
                self.include_router(router)

        if middlewares:
            if not isinstance(middlewares, Sequence):
                raise FastPubSubException("Your routers should be passed as a sequence")

            for middleware in middlewares:
                self.include_middleware(middleware)

    def _set_project_id(self, project_id: str) -> None:
        if self.project_id or not project_id:
            return

        self.project_id = project_id
        self._propagate_project_id()

    def _propagate_project_id(self) -> None:
        for router in self.routers:
            router._add_prefix(self.prefix)
            router._set_project_id(self.project_id)

        for publisher in self.publishers.values():
            publisher._set_project_id(self.project_id)

        for subscriber in self.subscribers.values():
            subscriber._set_project_id(self.project_id)

    def include_router(self, router: "PubSubRouter") -> None:
        """Includes a child router in the current router.

        Args:
            router: The router to include.
        """
        if not (router and isinstance(router, PubSubRouter)):
            raise FastPubSubException(f"Your routers must be of type {self.__class__.__name__}")

        if self == router:
            # V2: Create a algorithm to detect cycles on these routers.
            # For now, let us assume that the router is well configured
            # and this is the only error case.
            raise FastPubSubException(f"There is a cyclical reference on router {self.prefix}.")

        router._add_prefix(self.prefix)
        for existing_router in self.routers:
            if existing_router.prefix == router.prefix:
                raise FastPubSubException(
                    f"The prefix={router.prefix} is duplicated, it must be unique."
                )

        router._set_project_id(self.project_id)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

        self.routers.append(router)

    @validate_call(config=ConfigDict(strict=True))
    def subscriber(
        self,
        alias: str,
        *,
        topic_name: str,
        subscription_name: str,
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

        def decorator(func: AsyncDecoratedCallable) -> AsyncDecoratedCallable:
            ensure_async_callable_function(func)

            prefixed_alias = alias
            prefixed_subscription_name = subscription_name

            if self.prefix and isinstance(self.prefix, str):
                prefixed_alias = f"{self.prefix}.{prefixed_alias}"
                prefixed_subscription_name = f"{self.prefix}.{prefixed_subscription_name}"

            if prefixed_alias in self.subscribers:
                raise FastPubSubException(
                    f"The alias '{prefixed_alias}' already exists."
                    " The alias must be unique among all subscribers"
                )

            dead_letter_policy = None
            if dead_letter_topic:
                dead_letter_policy = DeadLetterPolicy(
                    topic_name=dead_letter_topic, max_delivery_attempts=max_delivery_attempts
                )

            retry_policy = MessageRetryPolicy(
                min_backoff_delay_secs=min_backoff_delay_secs,
                max_backoff_delay_secs=max_backoff_delay_secs,
            )

            delivery_policy = MessageDeliveryPolicy(
                filter_expression=filter_expression,
                ack_deadline_seconds=ack_deadline_seconds,
                enable_message_ordering=enable_message_ordering,
                enable_exactly_once_delivery=enable_exactly_once_delivery,
            )

            lifecycle_policy = LifecyclePolicy(autocreate=autocreate, autoupdate=autoupdate)

            control_flow_policy = MessageControlFlowPolicy(
                max_messages=max_messages,
            )

            subscriber_middlewares = list(middlewares) if middlewares else []
            for middleware in self.middlewares:
                subscriber_middlewares.append(middleware)

            subscriber = Subscriber(
                func=func,
                topic_name=topic_name,
                subscription_name=prefixed_subscription_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                lifecycle_policy=lifecycle_policy,
                control_flow_policy=control_flow_policy,
                dead_letter_policy=dead_letter_policy,
                middlewares=subscriber_middlewares,
            )
            subscriber._set_project_id(self.project_id)
            self.subscribers[prefixed_alias.lower()] = subscriber
            return func

        return decorator

    @validate_call(config=ConfigDict(strict=True))
    def publisher(self, topic_name: str) -> Publisher:
        """Returns a publisher for the given topic.

        Args:
            topic_name: The name of the topic.

        Returns:
            A publisher for the given topic.
        """
        publisher = self.publishers.get(topic_name)
        if not publisher:
            publisher = Publisher(topic_name=topic_name, middlewares=self.middlewares)
            publisher._set_project_id(self.project_id)
            self.publishers[topic_name] = publisher

        return publisher

    @validate_call(config=ConfigDict(strict=True))
    async def publish(
        self,
        topic_name: str,
        data: dict[str, Any] | str | bytes | BaseModel,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        """Publishes a message to the given topic.

        Args:
            topic_name: The name of the topic.
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
            autocreate: Whether to automatically create the topic if it does not exists.
        """
        publisher = self.publisher(topic_name=topic_name)
        await publisher.publish(
            data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate
        )

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        """Includes a middleware in the router.

        Args:
            middleware: The middleware to include.
        """
        for publisher in self.publishers.values():
            publisher.include_middleware(middleware)

        for subscriber in self.subscribers.values():
            subscriber.include_middleware(middleware)

        if middleware not in self.middlewares:
            self.middlewares.append(middleware)

        for router in self.routers:
            router.include_middleware(middleware)

    def _get_subscribers(self) -> dict[str, Subscriber]:
        subscribers: dict[str, Subscriber] = {}
        subscribers.update(self.subscribers)
        router: PubSubRouter
        for router in self.routers:
            router_subscribers = router._get_subscribers()
            subscribers.update(router_subscribers)

        return subscribers

    @validate_call
    def _add_prefix(self, prefix: str) -> None:
        if not prefix:
            return

        prefixes = OrderedDict()
        for new_prefix in prefix.split("."):
            prefixes[new_prefix] = True

        for old_prefix in self.prefix.split("."):
            prefixes[old_prefix] = True

        self.prefix = ".".join(list(prefixes.keys()))
        for router in self.routers:
            router._add_prefix(prefix=prefix)

        subscribers_to_realias = dict(self.subscribers)
        self.subscribers.clear()
        for alias, subscriber in subscribers_to_realias.items():
            subscriber._add_prefix(self.prefix)

            old_alias = alias.split(".")[-1]
            new_prefixed_alias = f"{self.prefix}.{old_alias}"
            self.subscribers[new_prefixed_alias] = subscriber
