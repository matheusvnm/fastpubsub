"""Publisher logic."""

import functools
import json

from pydantic import BaseModel

from fastpubsub.concurrency.utils import ensure_async_callable
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand


class Publisher:
    def __init__(self, project_id: str, topic_name: str, middlewares: list[type[BaseMiddleware]]):
        self.project_id = project_id
        self.topic_name = topic_name
        self.middlewares = middlewares
        self.middlewares: list[type[BaseMiddleware]] = []

        if middlewares:
            for middleware in middlewares:
                self.include_middleware(middleware)

    async def publish(
        self,
        data: BaseModel | dict | str | bytes | bytearray,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        callstack = self._build_callstack(autocreate=autocreate)
        serialized_message = self._serialize_message(data)
        await callstack.on_publish(serialized_message, ordering_key, attributes)

    def _build_callstack(self, autocreate: bool = True) -> BaseMiddleware | PublishMessageCommand:
        publish_command = PublishMessageCommand(
            project_id=self.project_id, topic_name=self.topic_name
        )

        original_call = publish_command.on_publish
        publish_command.on_publish = functools.partial(original_call, autocreate=autocreate)
        for middleware in reversed(self.middlewares):
            publish_command = middleware(next_call=publish_command)
        return publish_command

    def _serialize_message(self, data: BaseModel | dict | str | bytes | bytearray) -> bytes:
        if isinstance(data, bytes):
            return data

        if isinstance(data, bytearray):
            return bytes(data)

        if isinstance(data, str):
            return data.encode(encoding="utf-8")

        if isinstance(data, dict):
            json_data = json.dumps(data, indent=None, separators=(",", ":"))
            return json_data.encode(encoding="utf-8")

        if isinstance(data, BaseModel):
            json_data = data.model_dump_json(indent=None)
            return json_data.encode(encoding="utf-8")

        raise FastPubSubException(
            f"The message {data} is not serializable."
            "Please send as one of the following formats: BaseModel, dict, str, bytes or bytearray)"
        )

    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        if not (middleware and issubclass(middleware, BaseMiddleware)):
            raise FastPubSubException(f"The middleware should be a {BaseMiddleware.__name__} type.")

        if middleware in self.middlewares:
            return

        ensure_async_callable(middleware)
        self.middlewares.append(middleware)

    def set_project_id(self, project_id: str):
        self.project_id = project_id
