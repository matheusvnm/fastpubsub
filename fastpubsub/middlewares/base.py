"""Base classes for middlewares."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from fastpubsub.datastructures import Message


class BaseMiddleware:
    """Base class for middlewares.

    Your middlewares should extend this class if you want to
    implement your own middleware.
    """

    def __init__(self, next_call: Union["BaseMiddleware", None]):
        """Initializes the BaseMiddleware.

        Args:
            next_call: The next middleware or command in the chain.
        """
        self.next_call = next_call

    async def on_message(self, message: "Message") -> Any:
        """Handles a message.

        When extending this methods, you should always call
        `await super().on_message(...)` to continue the chain.

        Args:
            message: The message to handle.
        """
        if not self.next_call:
            return

        return await self.next_call.on_message(message)

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
        if not self.next_call:
            return

        return await self.next_call.on_publish(data, ordering_key, attributes)


class Middleware:
    """Wrapper class for middlewares.

    You should only use this class to create middlewares on class constructors.
    Its purpose is to only store the middleware information for delayed initiatization.
    """

    def __init__(self, cls: type[BaseMiddleware], *args: Any, **kwargs: Any) -> None:
        """Initializes the Middleware.

        Args:
            cls: The middleware class you want to initialize later.
            args: The middleware class positional arguments.
            kwargs: The middleware class keyword arguments.
        """
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def __iter__(self) -> Iterator[Any]:
        """Magic method for getting the middleware information as an iterator.

        Returns:
            An iterator with the middleware class, args and kwargs.
        """
        as_tuple = (self.cls, self.args, self.kwargs)
        return iter(as_tuple)

    def __repr__(self) -> str:
        """Magic method for getting the middleware representation.

        Returns:
            A formatted string with those information.
        """
        class_name = self.__class__.__name__
        args_strings = [f"{value!r}" for value in self.args]
        option_strings = [f"{key}={value!r}" for key, value in self.kwargs.items()]
        name = getattr(self.cls, "__name__", "")
        args_repr = ", ".join([name] + args_strings + option_strings)
        return f"{class_name}({args_repr})"

    def __eq__(self, other: object) -> bool:
        """Magic method for comparing different middlewares.

        Returns:
            A True if the middleware is equal or False otherwise.
        """
        if not isinstance(other, Middleware):
            return False

        return self.cls == other.cls
