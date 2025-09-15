from typing import Union
from fastpubsub.concurrency import ensure_async_callable
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.middlewares import BaseSubscriberMiddleware, HandleMessageCommand
from fastpubsub.types import AsyncCallable


class Subscriber:
    def __init__(
        self,
        func: AsyncCallable,
        project_id: str,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        lifecycle_policy: LifecyclePolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy,
        control_flow_policy: MessageControlFlowPolicy,
        middlewares: list[type[BaseSubscriberMiddleware]] = None,
    ):
        
        self.project_id = project_id
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.retry_policy = retry_policy
        self.lifecycle_policy = lifecycle_policy
        self.delivery_policy = delivery_policy
        self.dead_letter_policy = dead_letter_policy
        self.control_flow_policy = control_flow_policy
        self.handler = HandleMessageCommand(target=func)
        self.middlewares: list[type[BaseSubscriberMiddleware]] = []

        if middlewares:
            for middleware in middlewares:
                self.add_middleware(middleware)

    def add_middleware(self, middleware: type[BaseSubscriberMiddleware]) -> None:
        if not (middleware and issubclass(middleware, BaseSubscriberMiddleware)):
            return

        if middleware in self.middlewares:
            return

        ensure_async_callable(middleware)
        self.middlewares.append(middleware)

    @property
    def callback(self) -> Union[HandleMessageCommand, BaseSubscriberMiddleware]:
        callback = self.handler
        for middleware in reversed(self.middlewares):
            callback = middleware(callback)
        return callback            

    def set_project_id(self, project_id: str):
        self.project_id = project_id

    def add_prefix(self, new_prefix: str):
        self.subscription_name = f"{new_prefix}.{self.subscription_name}"
