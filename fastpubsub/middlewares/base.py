"""Base classes for middlewares."""

from abc import abstractmethod
from typing import Any, Union

from fastpubsub.datastructures import Message
from fastpubsub.pubsub.commands import HandleMessageCommand, PublishMessageCommand


class BaseMiddleware:
    """Base class for middlewares.

    Your middlewares should extend this class if you want to
    implement your own middleware.
    """

    def __init__(
        self, next_call: Union["BaseMiddleware", "PublishMessageCommand", "HandleMessageCommand"]
    ):
        """Initializes the BaseMiddleware.

        Args:
            next_call: The next middleware or command in the chain.
        """
        self.next_call = next_call

    @abstractmethod
    async def on_message(self, message: Message) -> Any:
        """Handles a message.

        When extending this methods, you should always call
        `await super().on_message(...)` to continue the chain.

        Args:
            message: The message to handle.
        """
        if isinstance(self.next_call, PublishMessageCommand):
            raise TypeError(f"Incorrect middleware stack build for {self.__class__.__name__}")

        if not self.next_call:
            return

        return await self.next_call.on_message(message)

    @abstractmethod
    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        """Handles a publish event.

        When extending this methods, you should always call
        `await super().on_publish(...)` to continue the chain.

        Args:
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
        """
        if isinstance(self.next_call, HandleMessageCommand):
            raise TypeError(f"Incorrect middleware stack build for {self.__class__.__name__}")

        if not self.next_call:
            return

        return await self.next_call.on_publish(data, ordering_key, attributes)
