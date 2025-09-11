from starconsumers._internal.types import DecoratedCallable, SubscribedCallable
from starconsumers.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from starconsumers.exceptions import StarConsumersException
from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.publisher import Publisher
from starconsumers.subscriber import Subscriber


class Registrator:
    def __init__(
        self, middlewares: list[type[BaseSubscriberMiddleware] | type[BasePublisherMiddleware]]
    ):
        self.prefix: str = ""
        self.project_id: str = ""
        self.publishers: dict[str, Publisher] = {}
        self.subscribers: dict[str, Subscriber] = {}
        self.middlewares = middlewares or []

    # TODO: Add param type check
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
        middlewares: list[type[BaseSubscriberMiddleware]] = None,
    ) -> SubscribedCallable:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            prefixed_alias = alias
            prefixed_subscription_name = subscription_name

            if self.prefix and isinstance(self.prefix, str):
                prefixed_alias = f"{self.prefix}.{prefixed_alias}"
                prefixed_subscription_name = f"{self.prefix}.{prefixed_subscription_name}"

            if prefixed_alias in self.subscribers:
                raise StarConsumersException(
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
                existing_subscriber_filter = existing_subscriber.delivery_policy.filter_expression
                if existing_subscriber_filter == filter_expression:
                    raise StarConsumersException(
                        f"The subscription '{prefixed_subscription_name}' for '{filter_expression=}' already exists."
                        " We only accept one handler per subscription and filter expression combination"
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
                project_id=self.project_id,
                topic_name=topic_name,
                subscription_name=prefixed_subscription_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                lifecycle_policy=lifecycle_policy,
                control_flow_policy=control_flow_policy,
                dead_letter_policy=dead_letter_policy,
                middlewares=subscriber_middlewares,
            )

            self.subscribers[prefixed_alias.casefold()] = subscriber
            return func

        return decorator

    # TODO: Add param type check
    def publisher(self, topic_name: str) -> Publisher:
        if topic_name not in self.publishers:
            publisher = Publisher(
                project_id=self.project_id, topic_name=topic_name, middlewares=self.middlewares
            )
            self.publishers[topic_name] = publisher

        return self.publishers[topic_name]

    async def publish(
        self,
        topic_name: str,
        data: dict,
        ordering_key: str = "",
        attributes: dict = None,
        autocreate: bool = True,
    ) -> None:
        publisher = self.publisher(topic_name)
        await publisher.publish(
            data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate
        )

    def add_middleware(
        self, middleware: type[BaseSubscriberMiddleware] | type[BasePublisherMiddleware]
    ) -> None:
        if not issubclass(
            middleware,
            (BaseSubscriberMiddleware | BasePublisherMiddleware),
        ):
            return

        if middleware not in self.middlewares:
            self.middlewares.append(middleware)

        for middleware in self.middlewares:
            for publisher in self.publishers.values():
                publisher.add_middleware(middleware)

            for subscriber in self.subscribers.values():
                subscriber.add_middleware(middleware)
