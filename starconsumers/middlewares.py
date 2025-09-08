"""Middleware implementations."""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from starconsumers._internal.types import DecoratedCallable


if TYPE_CHECKING:
    from starconsumers.datastructures import PubSubMessage


class HandlerItem:

    def __init__(self, *, target: DecoratedCallable):
        self.target = target
        
    def __call__(self, message: Any):
        return self.target(message)



@dataclass
class BaseSubscriberMiddleware:
    next_call: "BaseSubscriberMiddleware" | "HandlerItem"

    async def on_consume(self, message: "PubSubMessage"):
        if isinstance(self.next_call, HandlerItem):
            await self.next_call(message)

        return await self.next_call.on_consume(message=message)

@dataclass
class BasePublisherMiddleware:
    next_call: "BasePublisherMiddleware" 

    async def on_publish(self, data: dict, ordering_key: str = "", attributes: dict = None):
        # TODO: Build the wrapping logic
        return await self.next_call.on_publish(data=data, ordering_key=ordering_key, attributes=attributes)
