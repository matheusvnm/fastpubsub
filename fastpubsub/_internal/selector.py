"""Subscriber selection and pattern matching."""

import logging
from fnmatch import fnmatch

from fastpubsub.pubsub.subscriber import Subscriber

logger = logging.getLogger(__name__)


def _is_glob_pattern(pattern: str) -> bool:
    """Check if a string contains glob characters."""
    return any(c in pattern for c in ("*", "?"))


def _match_pattern(pattern: str, alias: str) -> bool:
    """Match a pattern against a subscriber alias.

    Supports three modes:
    - Exact match: ``orders.process``
    - Glob (fnmatch): ``orders.*``, ``order?``
    - Hierarchical glob: ``**`` matches zero or more
      dot-separated segments.

    Args:
        pattern: The user-provided pattern.
        alias: The subscriber alias to match against.

    Returns:
        True if the pattern matches the alias.
    """
    if not _is_glob_pattern(pattern):
        return pattern == alias

    return _match_segments(pattern, alias)


def _match_segments(pattern: str, alias: str) -> bool:
    """Match a glob pattern against an alias segment-wise.

    ``**`` matches zero or more dot-separated segments.
    ``*`` matches within a single segment (no dots).
    ``?`` matches a single character (not a dot).
    """
    pattern_parts = pattern.split(".")
    # Collapse consecutive ** segments to prevent exponential backtracking
    collapsed: list[str] = []
    for part in pattern_parts:
        if part == "**" and collapsed and collapsed[-1] == "**":
            continue
        collapsed.append(part)
    alias_parts = alias.split(".")
    return _match_parts(tuple(collapsed), 0, tuple(alias_parts), 0)


def _match_parts(
    pattern_parts: tuple[str, ...],
    pattern_index: int,
    alias_parts: tuple[str, ...],
    alias_index: int,
) -> bool:
    """Recursive matcher for dot-separated segments."""
    if pattern_index == len(pattern_parts) and alias_index == len(alias_parts):
        return True
    if pattern_index == len(pattern_parts):
        return False

    if pattern_parts[pattern_index] == "**":
        for skip in range(alias_index, len(alias_parts) + 1):
            if _match_parts(
                pattern_parts, pattern_index + 1, alias_parts, skip
            ):
                return True
        return False

    if alias_index == len(alias_parts):
        return False

    if fnmatch(alias_parts[alias_index], pattern_parts[pattern_index]):
        return _match_parts(
            pattern_parts, pattern_index + 1, alias_parts, alias_index + 1
        )

    return False


class SubscriberSelector:
    """Selects subscribers from a registry by patterns.

    Supports exact aliases, glob patterns (``*``, ``?``),
    and hierarchical glob (``**``) for matching against
    dot-separated subscriber aliases.

    Args:
        patterns: Set of patterns to match. Empty means all.
    """

    def __init__(self, patterns: set[str]) -> None:
        self._patterns = {p.lower().strip() for p in patterns if p.strip()}

    def select(self, subscribers: dict[str, Subscriber]) -> list[Subscriber]:
        """Select subscribers matching configured patterns.

        Args:
            subscribers: Registry of alias -> Subscriber.

        Returns:
            List of matched subscribers without duplicates.
        """
        if not self._patterns:
            logger.debug(
                "Running all subscribers: %s",
                list(subscribers.keys()),
            )
            return list(subscribers.values())

        seen_aliases: set[str] = set()
        result: list[Subscriber] = []

        for pattern in self._patterns:
            matched = False
            for alias, subscriber in subscribers.items():
                if alias in seen_aliases:
                    continue
                if _match_pattern(pattern, alias.lower()):
                    result.append(subscriber)
                    seen_aliases.add(alias)
                    matched = True

            if not matched:
                logger.warning(
                    "Pattern '%s' did not match any subscriber",
                    pattern,
                )

        return result
