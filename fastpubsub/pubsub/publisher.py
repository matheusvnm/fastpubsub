"""Publisher logic."""

import json
from collections.abc import MutableSequence
from typing import Any

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub.concurrency.utils import ensure_async_middleware
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand


class Publisher:
    """A class for publishing messages to a Pub/Sub topic."""

    def __init__(
        self,
        topic_name: str,
        project_id: str = "",
        middlewares: list[type[BaseMiddleware]] | None = None,
    ):
        """Initializes the Publisher.

        Args:
            topic_name: The name of the topic.
            project_id: An alternative project id to publish messages.
                        If set the broker's project id will be ignored.
            middlewares: A list of middlewares to apply.
        """
        self.topic_name = topic_name
        self.project_id = project_id
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
        serialized_message = await self._serialize_message(data)

        await callstack.on_publish(
            data=serialized_message, ordering_key=ordering_key, attributes=attributes
        )

    def _build_callstack(self, autocreate: bool = True) -> PublishMessageCommand | BaseMiddleware:
        callstack: PublishMessageCommand | BaseMiddleware = PublishMessageCommand(
            project_id=self.project_id, topic_name=self.topic_name, autocreate=autocreate
        )

        for middleware in reversed(self.middlewares):
            callstack = middleware(next_call=callstack)
        return callstack

    async def _serialize_message(self, data: BaseModel | dict[str, Any] | str | bytes) -> bytes:
        if isinstance(data, bytes):
            return data

        if isinstance(data, str):
            return data.encode(encoding="utf-8")

        if isinstance(data, dict):
            json_data = json.dumps(data, indent=None, separators=(",", ":"))
            return json_data.encode(encoding="utf-8")

        if isinstance(data, BaseModel):
            json_data = data.model_dump_json(indent=None)
            return json_data.encode(encoding="utf-8")

        raise FastPubSubException(
            f"The message {data} is not serializable. "
            "Please send as one of the following formats: BaseModel, dict, str or bytes."
        )

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

    def _set_project_id(self, project_id: str) -> None:
        if not self.project_id:
            self.project_id = project_id
