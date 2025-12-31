"""Context repository for storing and resolving runtime values.

This module provides thread-safe context storage for:
- Global values (persist across calls)
- Scoped values (per-request/per-call)

The ContextRepository supports dot-notation paths for nested access:
- "message" -> gets the message object
- "message.data" -> gets message.data attribute or stored value
- "message.attributes.user_id" -> gets nested attribute
"""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from pprint import pformat
from typing import Any

from fastpubsub.di.constants import EMPTY


class ContextError(KeyError):
    """Raised when a context key cannot be resolved."""

    def __init__(self, context: Any, field: str) -> None:
        """Initialize the ContextError.

        Args:
            context: The context that was searched.
            field: The field that was not found.
        """
        self.context = context
        self.field = field

    def __str__(self) -> str:
        """Return a formatted error message."""
        return "".join(
            (
                f"\n    Key `{self.field}` not found in the context\n    ",
                pformat(self.context),
            ),
        )


class ContextRepository:
    """Thread-safe context storage with nested key resolution.

    Supports two types of context:
    - Global: Persists across all calls (set_global/reset_global)
    - Scoped: Per-request using ContextVar (set_local/reset_local/scope)

    Key resolution:
    - Tries progressively shorter prefixes (longest match first)
    - Walks remaining path through attributes/dict keys
    - Example: "a.b.c" tries "a.b.c", then "a.b" + walk "c", then "a" + walk "b.c"
    """

    def __init__(self, initial: dict[str, Any] | None = None, /) -> None:
        """Initialize the context repository.

        Args:
            initial: Optional initial global context values.
        """
        self._global_context: dict[str, Any] = {"context": self} | (initial or {})
        self._scope_context: dict[str, ContextVar[Any]] = {}

    @property
    def context(self) -> dict[str, Any]:
        """Get merged view of global and scoped context."""
        return {
            **self._global_context,
            **{i: j.get() for i, j in self._scope_context.items()},
        }

    def set_global(self, key: str, v: Any) -> None:
        """Set a value in global context.

        Args:
            key: The key to set.
            v: The value to store.
        """
        self._global_context[key] = v

    def reset_global(self, key: str) -> None:
        """Remove a key from global context.

        Args:
            key: The key to remove.
        """
        self._global_context.pop(key, None)

    def set_local(self, key: str, value: Any) -> Token[Any]:
        """Set a scoped context value.

        Args:
            key: The key for the context variable.
            value: The value to set.

        Returns:
            Token for resetting the value later.
        """
        context_var = self._scope_context.get(key)
        if context_var is None:
            context_var = ContextVar(key, default=EMPTY)
            self._scope_context[key] = context_var
        return context_var.set(value)

    def reset_local(self, key: str, tag: Token[Any]) -> None:
        """Reset a scoped context value.

        Args:
            key: The key to reset.
            tag: Token from set_local.
        """
        self._scope_context[key].reset(tag)

    def get_local(self, key: str, default: Any = None) -> Any:
        """Get a scoped context value.

        Args:
            key: The key to get.
            default: Default if not found.

        Returns:
            The value or default.
        """
        if (context_var := self._scope_context.get(key)) is None:
            return default

        if (context_value := context_var.get()) is EMPTY:
            return default

        return context_value

    @contextmanager
    def scope(self, key: str, value: Any) -> Iterator[None]:
        """Context manager for scoped values.

        Args:
            key: The key to set.
            value: The value for this scope.

        Yields:
            None
        """
        token = self.set_local(key, value)
        try:
            yield
        finally:
            self.reset_local(key, token)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from context (global first, then scoped).

        Args:
            key: The key to get.
            default: Default if not found.

        Returns:
            The value or default.
        """
        if (glob := self._global_context.get(key, EMPTY)) is EMPTY:
            return self.get_local(key, default)
        return glob

    def __getattr__(self, name: str, /) -> Any:
        """Allow attribute-style access to context values."""
        return self.get(name)

    def resolve(self, argument: str) -> Any:
        """Resolve a dot-notation path to a value.

        Tries progressively shorter prefixes:
        1. Check if full path exists as context key
        2. Try shorter prefixes (a.b.c -> a.b -> a)
        3. Walk remaining path through attributes/dict keys

        Args:
            argument: Dot-notation path (e.g., "message.data.name").

        Returns:
            The resolved value.

        Raises:
            ContextError: If the path cannot be resolved.
        """
        parts = argument.split(".")

        # Try progressively shorter prefixes
        for i in range(len(parts), 0, -1):
            prefix = ".".join(parts[:i])
            if (v := self.get(prefix, EMPTY)) is not EMPTY:
                # Found a match, walk remaining path
                remaining_keys = parts[i:]
                for key in remaining_keys:
                    v = v[key] if isinstance(v, Mapping) else getattr(v, key)
                return v

        raise ContextError(self.context, parts[0])

    def clear(self) -> None:
        """Clear all context (global and scoped)."""
        self._global_context = {"context": self}
        self._scope_context.clear()


context_repo = ContextRepository()


def resolve_context_by_name(
    name: str,
    default: Any = EMPTY,
    initial: Callable[..., Any] | None = None,
) -> Any:
    """Resolve a context value by name with default/initial support.

    Args:
        name: The context key to resolve.
        default: Default value if not found.
        initial: Factory for initial value if not found.

    Returns:
        The resolved value, default, or initial value.
    """
    value: Any = EMPTY

    try:
        value = context_repo.resolve(name)

    except (KeyError, AttributeError):
        if EMPTY != default:
            value = default

        elif initial is not None:
            value = initial()
            context_repo.set_global(name, value)

    return value
