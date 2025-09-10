from dataclasses import dataclass
from typing import Any, Union

from starconsumers._internal.types import AsyncCallable
from starconsumers.datastructures import Message
from starconsumers.pubsub.pub import PubSubPublisherClient


@dataclass
class BaseSubscriberMiddleware:
    next_call: Union["BaseSubscriberMiddleware", "MessageHandleCommand"]

    async def __call__(self, message: Message) -> Any:
        return await self.next_call(message)


@dataclass
class BasePublisherMiddleware:
    next_call: Union["BasePublisherMiddleware", "MessagePublishCommand"]

    async def __call__(self, data: dict, ordering_key: str = "", attributes: dict = None) -> Any:
        return await self.next_call(data=data, ordering_key=ordering_key, attributes=attributes)


class MessageHandleCommand:
    def __init__(self, *, target: AsyncCallable):
        self.target = target

    async def __call__(self, message: Message) -> Any:
        # TODO: Add the serialization logic
        return await self.target(message)


class MessagePublishCommand:
    def __init__(self, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name

    async def __call__(self, data: dict, ordering_key: str = "", attributes: dict = None) -> None:
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=self.topic_name)
        client.publish(data=data, ordering_key=ordering_key, attributes=attributes)
