"""Subscriber selection and pattern matching."""

import logging
from fnmatch import fnmatch

from fastpubsub.pubsub.subscriber import Subscriber

logger = logging.getLogger(__name__)


def _is_glob_pattern(pattern: str) -> bool:
    """Check if a string contains glob characters."""
    return any(c in pattern for c in ("*", "?", "["))


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
    alias_parts = alias.split(".")
    return _match_parts(pattern_parts, 0, alias_parts, 0)


def _match_parts(
    pattern_parts: list[str],
    pi: int,
    alias_parts: list[str],
    ai: int,
) -> bool:
    """Recursive matcher for dot-separated segments."""
    if pi == len(pattern_parts) and ai == len(alias_parts):
        return True
    if pi == len(pattern_parts):
        return False

    if pattern_parts[pi] == "**":
        for skip in range(ai, len(alias_parts) + 1):
            if _match_parts(pattern_parts, pi + 1, alias_parts, skip):
                return True
        return False

    if ai == len(alias_parts):
        return False

    if fnmatch(alias_parts[ai], pattern_parts[pi]):
        return _match_parts(pattern_parts, pi + 1, alias_parts, ai + 1)

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
                if _match_pattern(pattern, alias):
                    result.append(subscriber)
                    seen_aliases.add(alias)
                    matched = True

            if not matched:
                logger.warning(
                    "Pattern '%s' did not match any subscriber",
                    pattern,
                )

        return result
