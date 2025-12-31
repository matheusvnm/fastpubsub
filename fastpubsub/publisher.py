"""Publisher logic."""

from collections.abc import MutableSequence
from typing import Any

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub.concurrency import ensure_async_middleware
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares import BaseMiddleware, _PublishMessageEncoderMiddleware
from fastpubsub.serialization import Serializer


class Publisher:
    """A class for publishing messages to a Pub/Sub topic."""

    def __init__(
        self,
        topic_name: str,
        project_id: str = "",
        middlewares: list[type[BaseMiddleware]] | None = None,
        serializer: Serializer | None = None,
    ):
        """Initializes the Publisher.

        Args:
            topic_name: The name of the topic.
            project_id: An alternative project id to publish messages.
                        If set the broker's project id will be ignored.
            middlewares: A list of middlewares to apply.
            serializer: The serializer for encoding messages.
                If None, inherits from router or broker.
        """
        self.topic_name = topic_name
        self.project_id = project_id
        self.serializer = serializer
        self.middlewares: list[type[BaseMiddleware]] = []

        if middlewares and isinstance(middlewares, MutableSequence):
            for middleware in middlewares:
                self.include_middleware(middleware)

    @validate_call(config=ConfigDict(strict=True))
    async def publish(
        self,
        data: dict[str, Any] | str | bytes | BaseModel,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        """Publishes a message to the topic.

        Args:
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
            autocreate: Whether to automatically create the topic.
        """
        callstack = self._build_callstack(autocreate=autocreate)
        await callstack.on_publish(data=data, ordering_key=ordering_key, attributes=attributes)

    def _build_callstack(self, autocreate: bool = True) -> BaseMiddleware:
        if not self.serializer:
            raise FastPubSubException("The serializer was not found. Maybe you set as None?")

        callstack: BaseMiddleware = _PublishMessageEncoderMiddleware(
            project_id=self.project_id,
            topic_name=self.topic_name,
            serializer=self.serializer,
            autocreate=autocreate,
        )

        for middleware in reversed(self.middlewares):
            callstack = middleware(next_call=callstack)
        return callstack

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        """Includes a middleware in the publisher.

        Args:
            middleware: The middleware to include.
        """
        if middleware in self.middlewares:
            return

        ensure_async_middleware(middleware)
        self.middlewares.append(middleware)

    def set_serializer(self, serializer: Serializer) -> None:
        """Set the serializer (used during propagation).

        Args:
            serializer: The MessageCodec to use for encoding.
        """
        if not self.serializer:
            self.serializer = serializer

    def _set_project_id(self, project_id: str) -> None:
        if not self.project_id:
            self.project_id = project_id
