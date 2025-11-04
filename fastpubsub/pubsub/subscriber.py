"""Subscriber logic."""

from collections.abc import Sequence

from pydantic import ConfigDict, validate_call

from fastpubsub.concurrency.utils import ensure_async_middleware
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import HandleMessageCommand
from fastpubsub.types import AsyncCallable


class Subscriber:
    """A class representing a Pub/Sub subscriber."""

    def __init__(
        self,
        func: AsyncCallable,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        lifecycle_policy: LifecyclePolicy,
        delivery_policy: MessageDeliveryPolicy,
        control_flow_policy: MessageControlFlowPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
        middlewares: Sequence[type[BaseMiddleware]] | None = None,
    ) -> None:
        """Initializes the Subscriber.

        Args:
            func: The function to call when a message is received.
            topic_name: The name of the topic to subscribe to.
            subscription_name: The name of the subscription.
            retry_policy: The retry policy for the subscription.
            lifecycle_policy: The lifecycle policy for the subscription.
            delivery_policy: The delivery policy for the subscription.
            control_flow_policy: The control flow policy for the subscription.
            dead_letter_policy: The dead-letter policy for the subscription.
            middlewares: A sequence of middlewares to apply.
        """
        self.project_id = ""
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

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        """Includes a middleware in the subscriber.

        Args:
            middleware: The middleware to include.
        """
        if middleware in self.middlewares:
            return

        ensure_async_middleware(middleware)
        self.middlewares.append(middleware)

    def _build_callstack(self) -> HandleMessageCommand | BaseMiddleware:
        callstack: HandleMessageCommand | BaseMiddleware = self.handler
        for middleware in reversed(self.middlewares):
            callstack = middleware(callstack)
        return callstack

    @property
    def name(self) -> str:
        """The name of the subscriber."""
        return self.handler.target.__name__

    def _set_project_id(self, project_id: str) -> None:
        self.project_id = project_id

    def _add_prefix(self, new_prefix: str) -> None:
        subscription_name = self.subscription_name.split(".")[-1]
        self.subscription_name = f"{new_prefix}.{subscription_name}"
