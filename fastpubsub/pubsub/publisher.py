"""Publisher logic."""

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub.concurrency.utils import ensure_async_middleware
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand


class Publisher:
    def __init__(self, topic_name: str, middlewares: list[type[BaseMiddleware]]):
        self.project_id = ""
        self.topic_name = topic_name
        self.middlewares: list[type[BaseMiddleware]] = []

        if middlewares:
            for middleware in middlewares:
                self.include_middleware(middleware)

    @validate_call(config=ConfigDict(strict=True))
    async def publish(
        self,
        data: BaseModel | dict[str, Any] | str | bytes,
        ordering_key: str | None = None,
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        callstack = self.build_callstack(autocreate=autocreate)
        serialized_message = self._serialize_message(data)
        await callstack.on_publish(
            data=serialized_message, ordering_key=ordering_key, attributes=attributes
        )

    def build_callstack(self, autocreate: bool = True) -> PublishMessageCommand | BaseMiddleware:
        callstack: PublishMessageCommand | BaseMiddleware = PublishMessageCommand(
            project_id=self.project_id, topic_name=self.topic_name, autocreate=autocreate
        )

        for middleware in reversed(self.middlewares):
            callstack = middleware(next_call=callstack)
        return callstack

    def _serialize_message(self, data: BaseModel | dict[str, Any] | str | bytes) -> bytes:
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
        if middleware in self.middlewares:
            return

        ensure_async_middleware(middleware)
        self.middlewares.append(middleware)

    def set_project_id(self, project_id: str) -> None:
        self.project_id = project_id
