"""Subscriber logic."""

from starconsumers.datastructures import (
    DeadLetterPolicy,
    DeliveryPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    RetryPolicy,
)
from starconsumers.middlewares import BaseSubscriberMiddleware, HandlerItem
from starconsumers._internal.types import DecoratedCallable


class Subscriber:
    def __init__(
        self,
        func: DecoratedCallable,
        project_id: str,
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
        self.project_id = project_id
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
        # TODO: Add middleware logic
        return self._handler


    def add_middleware(self, middleware: BaseSubscriberMiddleware) -> None:
        if not (middleware and isinstance(middleware, BaseSubscriberMiddleware)):
            return
        
        if middleware in self.middlewares:
            return
        
        self.middlewares.append(middleware)

    def set_project_id(self, project_id: str):
        if project_id and isinstance(project_id, str):
            self._project_id = project_id