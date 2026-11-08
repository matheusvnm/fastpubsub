"""Publisher logic."""

import json
from collections.abc import MutableSequence, Sequence
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, validate_call

from fastpubsub._internal.concurrency import ensure_async_middleware
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares import (
    Middleware,
    PublishMessageSerializerMiddleware,
)
from fastpubsub.middlewares.base import BaseMiddleware


def has_valid_publish_types(
    data: object, ordering_key: object, attributes: object, autocreate: object
) -> bool:
    """Check the publish arguments without Pydantic.

    The check accepts only exact built-in types, so it never accepts a
    value that the strict Pydantic validation rejects.

    Args:
        data: The message data.
        ordering_key: The ordering key.
        attributes: The message attributes.
        autocreate: The flag to create the topic.

    Returns:
        True when all arguments have a valid type.
    """
    if type(ordering_key) is not str or type(autocreate) is not bool:
        return False
    if attributes is not None:
        if type(attributes) is not dict:
            return False
        for key, value in attributes.items():
            if type(key) is not str or type(value) is not str:
                return False
    data_type = type(data)
    if data_type is bytes or data_type is str:
        return True
    if data_type is dict:
        keys = cast(dict[object, object], data).keys()
        return all(type(key) is str for key in keys)
    return isinstance(data, BaseModel)


class Publisher:
    """A class for publishing messages to a Pub/Sub topic."""

    def __init__(
        self,
        topic_name: str,
        project_id: str = "",
        middlewares: Sequence[Middleware] = (),
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
        self.middlewares: MutableSequence[Middleware] = []
        self._callstacks: dict[bool, BaseMiddleware] = {}

        if middlewares and isinstance(middlewares, Sequence):
            for middleware, args, kwargs in middlewares:
                self.include_middleware(middleware, *args, **kwargs)

    async def publish(
        self,
        data: dict[str, Any] | str | bytes | BaseModel,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        """Publishes a message to the topic.

        The common argument types skip the Pydantic validation. Other
        values go through the strict Pydantic validation, which raises a
        ``ValidationError`` for invalid values.

        Args:
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
            autocreate: Whether to automatically create the topic.
        """
        if has_valid_publish_types(data, ordering_key, attributes, autocreate):
            await self._send(data, ordering_key, attributes, autocreate)
            return
        await self._validate_and_send(
            data, ordering_key, attributes, autocreate
        )

    @validate_call(config=ConfigDict(strict=True))
    async def _validate_and_send(
        self,
        data: dict[str, Any] | str | bytes | BaseModel,
        ordering_key: str = "",
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        """Validates the argument types strictly, then publishes.

        Args:
            data: The message payload.
            ordering_key: The ordering key.
            attributes: The message attributes.
            autocreate: Create the topic if it does not exist.
        """
        await self._send(data, ordering_key, attributes, autocreate)

    async def _send(
        self,
        data: dict[str, Any] | str | bytes | BaseModel,
        ordering_key: str,
        attributes: dict[str, str] | None,
        autocreate: bool,
    ) -> None:
        """Serializes the payload and sends it through the middlewares.

        Args:
            data: The message payload.
            ordering_key: The ordering key.
            attributes: The message attributes.
            autocreate: Create the topic if it does not exist.
        """
        callstack = self._get_callstack(autocreate=autocreate)
        serialized_message = await self._serialize_message(data)

        await callstack.on_publish(
            data=serialized_message,
            ordering_key=ordering_key,
            attributes=attributes,
        )

    def _get_callstack(self, autocreate: bool) -> BaseMiddleware:
        """Gets the cached middleware chain for an ``autocreate`` value.

        Args:
            autocreate: Create the topic if it does not exist.

        Returns:
            The first middleware of the chain.
        """
        callstack = self._callstacks.get(autocreate)
        if callstack is None:
            callstack = self._build_callstack(autocreate=autocreate)
            self._callstacks[autocreate] = callstack
        return callstack

    def _build_callstack(self, autocreate: bool = True) -> BaseMiddleware:
        """Builds the middleware chain that ends with the publish.

        Args:
            autocreate: Create the topic if it does not exist.

        Returns:
            The first middleware of the chain.
        """
        callstack: BaseMiddleware = PublishMessageSerializerMiddleware(
            None,
            project_id=self.project_id,
            topic_name=self.topic_name,
            autocreate=autocreate,
        )

        for middleware, args, kwargs in reversed(self.middlewares):
            callstack = middleware(callstack, *args, **kwargs)
        return callstack

    @staticmethod
    async def _serialize_message(
        data: BaseModel | dict[str, Any] | str | bytes,
    ) -> bytes:
        """Converts a payload to bytes.

        Bytes stay the same. Strings use UTF-8. Dictionaries and Pydantic
        models become compact JSON.

        Args:
            data: The message payload.

        Returns:
            The payload as bytes.

        Raises:
            FastPubSubException: The payload has an unsupported type.
        """
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
            "Please send as one of the following formats: "
            "BaseModel, dict, str or bytes."
        )

    @validate_call(config=ConfigDict(strict=True))
    def include_middleware(
        self, middleware: type[BaseMiddleware], *args: Any, **kwargs: Any
    ) -> None:
        """Includes a middleware in the publisher.

        Args:
            middleware: The middleware to include.
            args: The positional arguments used on middleware instantiation.
            kwargs: The keyword  arguments used on middleware instantiation.
        """
        ensure_async_middleware(middleware)

        wrapper_middleware = Middleware(middleware, *args, **kwargs)
        if wrapper_middleware in self.middlewares:
            return

        self.middlewares.append(wrapper_middleware)
        self._callstacks.clear()

    def _set_project_id(self, project_id: str) -> None:
        """Sets the project ID if the publisher has none.

        The method also clears the cached middleware chains, because they
        hold the project ID.

        Args:
            project_id: The project ID of the router.
        """
        if not self.project_id:
            self.project_id = project_id
            self._callstacks.clear()
