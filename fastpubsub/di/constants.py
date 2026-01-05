"""Constants for serialization module."""

from typing import Any


class _EmptyType:
    """Sentinel for empty/missing values."""

    def __repr__(self) -> str:
        return "EMPTY"

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _EmptyType)


EMPTY: Any = _EmptyType()
