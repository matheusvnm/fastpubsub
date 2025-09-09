"""Middleware implementations."""

from dataclasses import dataclass
import inspect
from typing import Any, Union

from starconsumers._internal.types import AsyncCallable
from starconsumers.datastructures import Message


class HandlerItem:
    def __init__(self, *, target: AsyncCallable):
        self.target = target

    async def __call__(self, message: Message) -> Any:
        # TODO: Add the serialization logic
        return await self.target(message)


@dataclass
class BaseSubscriberMiddleware:
    next_call: Union["BaseSubscriberMiddleware", "HandlerItem"]

    async def __call__(self, message: Message):
        return await self.next_call(message)


@dataclass
class BasePublisherMiddleware:
    next_call: "BasePublisherMiddleware"

    async def __call__(self, data: dict, ordering_key: str = "", attributes: dict = None):
        # TODO: Build the wrapping logic
        return await self.next_call(data=data, ordering_key=ordering_key, attributes=attributes)
