


from typing import Union
from old.starconsumers.datastructures import MessageDeliveryPolicy, MessageRetryPolicy, SubscriptionLifecyclePolicy
from starconsumers.datastructures import DeadLetterPolicy, MessageControlFlowPolicy
from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.publisher import Publisher
from starconsumers.subscriber import Subscriber
from starconsumers._internal.types import DecoratedCallable, SubscribedCallable


class Registrator:

    def __init__(self, middlewares: list[Union[BaseSubscriberMiddleware, BasePublisherMiddleware]]):
        self.prefix: str = ""
        self.project_id: str = ""
        self.publishers: dict[str, Publisher] = {}
        self.subscribers: dict[str, Subscriber] = {}

        self.middlewares = middlewares
    
    def subscriber(self,
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
    ) -> SubscribedCallable:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
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

            lifecycle_policy = SubscriptionLifecyclePolicy(
                autocreate=autocreate, autoupdate=autoupdate
            )

            control_flow_policy = MessageControlFlowPolicy(
                max_messages=max_messages,
                max_bytes=max_messages_bytes,
            )

            prefixed_alias = alias
            prefixed_topic_name = topic_name
            prefixed_subscription_name = subscription_name

            if self.prefix and isinstance(self.prefix, str):
                prefixed_alias = f"{self.prefix}.{prefixed_alias}"
                prefixed_topic_name = f"{self.prefix}.{prefixed_topic_name}"
                prefixed_subscription_name = f"{self.prefix}.{prefixed_subscription_name}"

            publisher_middlewares = []
            for middleware in self.middlewares:
                if isinstance(middleware, BasePublisherMiddleware):
                    publisher_middlewares.append(middleware)     

            subscriber = Subscriber(
                func=func,
                project_id=self.project_id,
                topic_name=prefixed_topic_name,
                subscription_name=prefixed_subscription_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                lifecycle_policy=lifecycle_policy,
                control_flow_policy=control_flow_policy,
                dead_letter_policy=dead_letter_policy,
            )

            for middleware in self.middlewares:
                subscriber.add_middleware(middleware)

            self.subscribers[prefixed_alias.casefold()] = subscriber
            return func

        return decorator
    

    def publisher(self, topic_name: str) -> Publisher:
        prefixed_topic_name = topic_name
        if self.prefix and isinstance(self.prefix, str):
            prefixed_topic_name = f"{self.prefix}.{prefixed_topic_name}"

        if prefixed_topic_name not in self.publishers:
            publisher = Publisher(project_id=self.project_id, 
                                  topic_name=prefixed_topic_name, 
                                  middlewares=[])

            for middleware in self.middlewares:
                publisher.add_middleware(middleware)

            self.publishers[prefixed_topic_name] = publisher

        return self.publishers[prefixed_topic_name]

    async def publish(self, topic_name: str, data: dict, ordering_key: str = "", attributes: dict = None) -> None:
        publisher = self.publisher(topic_name)
        await publisher.publish(data=data, ordering_key=ordering_key, attributes=attributes)
