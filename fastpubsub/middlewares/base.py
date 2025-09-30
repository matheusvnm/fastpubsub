from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Union

from fastpubsub.datastructures import Message
from fastpubsub.pubsub.commands import HandleMessageCommand, PublishMessageCommand


@dataclass
class BaseMiddleware:
    def __init__(
        self, next_call: Union["BaseMiddleware", "PublishMessageCommand", "HandleMessageCommand"]
    ):
        self.next_call = next_call

    @abstractmethod
    async def on_message(self, message: Message) -> Any:
        if isinstance(self.next_call, PublishMessageCommand):
            raise TypeError(f"Incorrect middleware stack build for {self.__class__.__name__}")

        if not self.next_call:
            return

        return await self.next_call.on_message(message)

    @abstractmethod
    async def on_publish(
        self, data: bytes, ordering_key: str | None, attributes: dict[str, str] | None
    ) -> Any:
        if isinstance(self.next_call, HandleMessageCommand):
            raise TypeError(f"Incorrect middleware stack build for {self.__class__.__name__}")

        if not self.next_call:
            return

        return await self.next_call.on_publish(data, ordering_key, attributes)
