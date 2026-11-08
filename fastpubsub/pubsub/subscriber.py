"""Subscriber logic."""

from collections.abc import MutableSequence, Sequence
from typing import Any

from pydantic import ConfigDict, validate_call

from fastpubsub.concurrency.utils import ensure_async_middleware
from fastpubsub.datastructures import (
    DeadLetterPolicy,
    LifecyclePolicy,
    MessageControlFlowPolicy,
    MessageDeliveryPolicy,
    MessageRetryPolicy,
)
from fastpubsub.middlewares import (
    HandleMessageSerializerMiddleware,
    Middleware,
)
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.types import MessageHandler


class Subscriber:
    """A class representing a Pub/Sub subscriber."""

    def __init__(
        self,
        func: MessageHandler,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        lifecycle_policy: LifecyclePolicy,
        delivery_policy: MessageDeliveryPolicy,
        control_flow_policy: MessageControlFlowPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
        middlewares: Sequence[Middleware] = (),
        project_id: str = "",
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
            project_id: An alternative project id to create a subscription
            and consume messages from.
            If set the broker's project id will be ignored.
        """
        self.func = func
        self.project_id = project_id
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.retry_policy = retry_policy
        self.lifecycle_policy = lifecycle_policy
        self.delivery_policy = delivery_policy
        self.dead_letter_policy = dead_letter_policy
        self.control_flow_policy = control_flow_policy
        self.middlewares: MutableSequence[Middleware] = []
        self._callstack: BaseMiddleware | None = None

        if middlewares and isinstance(middlewares, Sequence):
            for middleware, args, kwargs in middlewares:
                self.include_middleware(middleware, *args, **kwargs)

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(
        self, middleware: type[BaseMiddleware], *args: Any, **kwargs: Any
    ) -> None:
        """Includes a middleware in the subscriber.

        Args:
            middleware: The middleware to include.
            args: The positional arguments used on middleware instantiation.
            kwargs: The keyword arguments used on middleware instantiation.
        """
        ensure_async_middleware(middleware)

        wrapper_middleware = Middleware(middleware, *args, **kwargs)
        if wrapper_middleware in self.middlewares:
            return

        self.middlewares.append(wrapper_middleware)
        self._callstack = None

    @property
    def callstack(self) -> BaseMiddleware:
        """The middleware chain that handles each message.

        The chain is built once and reused for all messages.
        """
        if self._callstack is None:
            self._callstack = self._build_callstack()
        return self._callstack

    def _build_callstack(self) -> BaseMiddleware:
        """Builds the middleware chain that ends with the handler.

        Returns:
            The first middleware of the chain.
        """
        callstack: BaseMiddleware = HandleMessageSerializerMiddleware(
            None, self.func
        )
        for middleware, args, kwargs in reversed(self.middlewares):
            callstack = middleware(callstack, *args, **kwargs)
        return callstack

    @property
    def name(self) -> str:
        """The name of the subscriber."""
        return getattr(self.func, "__name__", "")

    def _set_project_id(self, project_id: str) -> None:
        """Sets the project ID if the subscriber has none.

        Args:
            project_id: The project ID of the router.
        """
        if not self.project_id:
            self.project_id = project_id

    def _add_prefix(self, new_prefix: str) -> None:
        """Replaces the prefix of the subscription name.

        Args:
            new_prefix: The full prefix of the router.
        """
        subscription_name = self.subscription_name.split(".")[-1]
        self.subscription_name = f"{new_prefix}.{subscription_name}"

    def __str__(self) -> str:
        """Returns a formatted str representation for the object."""
        return (
            f"Subscriber(name={self.name}, "
            f"topic_name={self.topic_name}, "
            f"subscription_name={self.subscription_name})"
        )
