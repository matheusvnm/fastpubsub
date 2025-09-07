"""Subscriber logic."""

from starconsumers.datastructures import (
    DeadLetterPolicy,
    DeliveryPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    RetryPolicy,
)
from starconsumers.middlewares import BaseSubscriberMiddleware, HandlerItem
from starconsumers.types import DecoratedCallable


class Subscriber:
    def __init__(
        self,
        func: DecoratedCallable,
        topic_name: str,
        subscription_name: str,
        retry_policy: RetryPolicy,
        lifecycle_policy: LifecyclePolicy,
        delivery_policy: DeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy,
        control_flow_policy: MessageControlFlowPolicy,
        middlewares: list[BaseSubscriberMiddleware] = [],
    ):
        self._handler = HandlerItem(target=func)
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.retry_policy = retry_policy
        self.lifecycle_policy = lifecycle_policy
        self.delivery_policy = delivery_policy
        self.dead_letter_policy = dead_letter_policy
        self.control_flow_policy = control_flow_policy
        self.middlewares = middlewares

    @property
    def handler(self) -> HandlerItem:
        return self._handler


    def add_middleware(self, middleware: BaseSubscriberMiddleware) -> None:
        if not (middleware and isinstance(middleware, BaseSubscriberMiddleware)):
            return
        
        self.middlewares.append(middleware)