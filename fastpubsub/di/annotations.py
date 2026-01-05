"""FastDepends annotations for extracting data from messages.

This module provides CustomField annotations for dependency injection:
- Header() - Extract from message attributes/headers
- Body() - Extract from decoded message body
- Context() - Extract from context by key

Usage:
    @broker.subscriber(...)
    async def handler(
        # Extract from body (decoded message.data)
        name: Annotated[str, Body()],

        # Extract from headers (message.attributes)
        user_id: Annotated[str, Header()],

        # With validation constraints
        age: Annotated[int, Body(), Field(gt=0)],

        # Raw message access
        data: MessageData,
        attrs: MessageAttributes,
    ):
        ...
"""

from collections.abc import Callable
from typing import Any

from fast_depends.library import CustomField

from fastpubsub.di.constants import EMPTY
from fastpubsub.di.repository import resolve_context_by_name


class Context(CustomField):
    """CustomField for extracting values from context.

    Resolves values from the global ContextRepo using dot-notation paths.
    Supports nested access (e.g., "message.data.user.name").

    Attributes:
        name: The context key to resolve.
        default: Default value if key not found.
        prefix: Prefix added to the key.
        initial: Factory for initial value if not found.
    """

    param_name: str

    def __init__(
        self,
        real_name: str = "",
        *,
        default: Any = EMPTY,
        initial: Callable[..., Any] | None = None,
        cast: bool = False,
        prefix: str = "",
    ) -> None:
        """Initialize the Context field.

        Args:
            real_name: The key name to look up. If empty, uses parameter name.
            default: Default value if key not found.
            initial: Factory function to create initial value.
            cast: Whether to cast the value to parameter type.
            prefix: Prefix added to the key (e.g., "message.attributes.").
        """
        self.name = real_name
        self.default = default
        self.prefix = prefix
        self.initial = initial
        super().__init__(
            cast=cast,
            required=(default is EMPTY),
        )

    def use(self, /, **kwargs: Any) -> dict[str, Any]:
        """Resolve the context value and add to kwargs.

        This method:
        1. Tries to resolve the full path (prefix + name/param_name)
        2. If not found and there's a prefix, falls back to the prefix source
           (enables Pydantic models to receive entire dicts)

        Args:
            **kwargs: Current keyword arguments.

        Returns:
            Updated kwargs with resolved value.
        """
        name = f"{self.prefix}{self.name or self.param_name}"

        v = resolve_context_by_name(
            name=name,
            default=EMPTY,
            initial=None,
        )

        # Fallback to source dict for Pydantic model support
        if v is EMPTY and self.prefix:
            source_path = self.prefix.rstrip(".")
            v = resolve_context_by_name(
                name=source_path,
                default=self.default,
                initial=self.initial,
            )

        if v is EMPTY:
            v = resolve_context_by_name(
                name=name,
                default=self.default,
                initial=self.initial,
            )

        if v is not EMPTY:
            kwargs[self.param_name] = v
        else:
            kwargs.pop(self.param_name, None)

        return kwargs


def Header(
    real_name: str = "",
    *,
    cast: bool = True,
    default: Any = EMPTY,
) -> Any:
    """Extract a value from message attributes (headers).

    Creates a Context with prefix "message.attributes." for
    extracting values from the message's attribute dict.

    Args:
        real_name: The attribute key. If empty, uses parameter name.
        cast: Whether to cast the value to parameter type.
        default: Default value if key not found.

    Returns:
        Context configured for attribute extraction.

    Example:
        @broker.subscriber(...)
        async def handler(
            user_id: Annotated[str, Header()],
            trace_id: Annotated[str, Header("x-trace-id")],
        ):
            ...
    """
    return Context(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix="message.attributes.",
    )


def Body(
    real_name: str = "",
    *,
    cast: bool = True,
    default: Any = EMPTY,
) -> Any:
    """Extract a value from the decoded message body.

    Creates a Context with prefix "message.decoded_data." for
    extracting values from the decoded message body.

    Args:
        real_name: The body key. If empty, uses parameter name.
        cast: Whether to cast the value to parameter type.
        default: Default value if key not found.

    Returns:
        Context configured for body extraction.

    Example:
        @broker.subscriber(...)
        async def handler(
            name: Annotated[str, Body()],
            user: Annotated[UserModel, Body()],  # Gets entire body as dict
        ):
            ...
    """
    return Context(
        real_name=real_name,
        cast=cast,
        default=default,
        prefix="message.decoded_data.",
    )
