from abc import abstractmethod
from collections import OrderedDict
from typing import Any

from pydantic import BaseModel

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
from fastpubsub.types import DecoratedCallable, SubscribedCallable


class BaseRouter:
    def __init__(self, prefix: str = ""):
        self.prefix: str = prefix
        self.routers: list[BaseRouter] = []
        self.publishers: dict[str, Publisher] = {}
        self.subscribers: dict[str, Subscriber] = {}
        self.middlewares: list[type[BaseMiddleware]] = []

    @abstractmethod
    def include_router(self, router: "BaseRouter") -> None: ...

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
        max_messages_bytes: int = 100 * 1024 * 1024,
        middlewares: tuple[type[BaseMiddleware]] | Any = None,
    ) -> SubscribedCallable:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
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

            existing_subscriber = next(
                filter(
                    lambda sub: sub.subscription_name == prefixed_subscription_name,
                    self.subscribers.values(),
                ),
                None,
            )

            if existing_subscriber:
                raise FastPubSubException(
                    f"The subscription '{prefixed_subscription_name}' "
                    "must be unique among handlers."
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
                max_bytes=max_messages_bytes,
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

            self.subscribers[prefixed_alias.lower()] = subscriber
            return func

        return decorator

    def publisher(self, topic_name: str) -> Publisher:
        if topic_name not in self.publishers:
            publisher = Publisher(topic_name=topic_name, middlewares=self.middlewares)
            self.publishers[topic_name] = publisher

        return self.publishers[topic_name]

    async def publish(
        self,
        topic_name: str,
        data: BaseModel | dict[str, Any] | str | bytes | bytearray,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        publisher = self.publisher(topic_name)
        await publisher.publish(
            data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate
        )

    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
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

        router: BaseRouter
        for router in self.routers:
            router_subscribers = router._get_subscribers()
            subscribers.update(router_subscribers)

        return subscribers

    def add_prefix(self, prefix: str) -> None:
        if not prefix:
            return

        prefixes = OrderedDict()
        for new_prefix in prefix.split("."):
            prefixes[new_prefix] = True

        for old_prefix in self.prefix.split("."):
            prefixes[old_prefix] = True

        self.prefix = ".".join(list(prefixes.keys()))
        subscribers_to_realias = dict(self.subscribers)

        self.subscribers.clear()
        for alias, subscriber in subscribers_to_realias.items():
            subscriber.add_prefix(self.prefix)

            old_alias = alias.split(".")[-1]
            new_prefixed_alias = f"{self.prefix}.{old_alias}"
            self.subscribers[new_prefixed_alias] = subscriber
