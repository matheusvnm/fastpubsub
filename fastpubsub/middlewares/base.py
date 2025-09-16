from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from fastpubsub.datastructures import Message
from fastpubsub.pubsub.commands import HandleMessageCommand, PublishMessageCommand

# TODO: Unificar os middlewares de sub/pub
# TODO: Publisher -> on_publish
# TODO: Subscriber -> on_message


@dataclass
class BaseMiddleware:
    next_call: Union["BaseMiddleware", "PublishMessageCommand", "HandleMessageCommand"]

    @abstractmethod
    async def on_message(self, message: Message):
        return await self.next_call.on_message(message)

    @abstractmethod
    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        return await self.next_call.on_publish(data, ordering_key, attributes)
