from collections.abc import Callable
from typing import Any

from pydantic import validate_call

from starconsumers.datastructures import (
    DeadLetterPolicy,
    DecoratedCallable,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageMiddleware,
    MessageRetryPolicy,
    MiddlewaresRegister,
    SubscriptionLifecyclePolicy,
    Task,
    TasksRegister,
    TopicSubscription,
)


class TopicConsumer:
    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name

        self.tasks_register = TasksRegister()
        self.middlewares_register = MiddlewaresRegister()

    @validate_call
    def task(
        self,
        name: str,
        *,
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
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        def decorator(handler: DecoratedCallable) -> DecoratedCallable:
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

            subscription = TopicSubscription(
                name=subscription_name,
                project_id=self.project_id,
                topic_name=self.topic_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                lifecycle_policy=lifecycle_policy,
                control_flow_policy=control_flow_policy,
                dead_letter_policy=dead_letter_policy,
            )

            task = Task(
                handler=handler,
                subscription=subscription,
            )

            self.tasks_register.register(name, task)
            return handler

        return decorator

    def add_middleware(
        self, middleware: type[MessageMiddleware], *args: list[Any], **kwargs: dict[str, Any]
    ) -> None:
        self.middlewares_register.register(middleware, *args, **kwargs)
