from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.datastructures import Message
from fastpubsub.types import AsyncCallable


@dataclass
class BaseSubscriberMiddleware(ABC):
    next_call: Union["BaseSubscriberMiddleware", "HandleMessageCommand"]

    @abstractmethod
    async def __call__(self, message: Message) -> Any:
        return await self.next_call(message)


@dataclass
class BasePublisherMiddleware(ABC):
    next_call: Union["BasePublisherMiddleware", "PublishMessageCommand"]

    @abstractmethod
    async def __call__(
        self, data: dict, ordering_key: str, attributes: dict, autocreate: bool
    ) -> Any:
        return await self.next_call(
            data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate
        )


class HandleMessageCommand:
    def __init__(self, *, target: AsyncCallable):
        self.target = target

    async def __call__(self, message: Message) -> Any:
        # TODO: Add the serialization logic
        return await self.target(message)


class PublishMessageCommand:
    def __init__(self, *, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name

    async def __call__(
        self, data: dict, ordering_key: str, attributes: dict, autocreate: bool
    ) -> None:
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=self.topic_name)
        if autocreate:
            client.create_topic()

        client.publish(data=data, ordering_key=ordering_key, attributes=attributes)
