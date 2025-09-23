from fastpubsub.concurrency.utils import ensure_async_middleware
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import HandleMessageCommand
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
        control_flow_policy: MessageControlFlowPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
        middlewares: list[type[BaseMiddleware]] | None = None,
    ) -> None:
        self.project_id = project_id
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.retry_policy = retry_policy
        self.lifecycle_policy = lifecycle_policy
        self.delivery_policy = delivery_policy
        self.dead_letter_policy = dead_letter_policy
        self.control_flow_policy = control_flow_policy
        self.handler = HandleMessageCommand(target=func)
        self.middlewares: list[type[BaseMiddleware]] = []

        if middlewares:
            for middleware in middlewares:
                self.include_middleware(middleware)

    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        if not (middleware and issubclass(middleware, BaseMiddleware)):
            raise FastPubSubException(f"The middleware should be a {BaseMiddleware.__name__} type.")

        if middleware in self.middlewares:
            return

        ensure_async_middleware(middleware)
        self.middlewares.append(middleware)

    @property
    def callback(self) -> HandleMessageCommand | BaseMiddleware:
        callback: HandleMessageCommand | BaseMiddleware = self.handler
        for middleware in reversed(self.middlewares):
            callback = middleware(callback)
        return callback

    @property
    def name(self) -> str:
        return self.handler.target.__name__

    def set_project_id(self, project_id: str) -> None:
        self.project_id = project_id

    def add_prefix(self, new_prefix: str) -> None:
        self.subscription_name = f"{new_prefix}.{self.subscription_name}"
