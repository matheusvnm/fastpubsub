"""Root conftest with shared fixtures for FastPubSub tests."""

from __future__ import annotations

import os
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.router import PubSubRouter

if TYPE_CHECKING:
    from collections.abc import Callable
    from unittest.mock import MagicMock


@pytest.fixture
def project_id() -> str:
    """Get test project ID from environment or default."""
    return os.getenv("CLOUDSDK_CORE_PROJECT", "fastpubsub-test-project")


@pytest.fixture
def unique_topic() -> str:
    """Generate unique topic name for test isolation."""
    return f"test-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_subscription() -> str:
    """Generate unique subscription name for test isolation."""
    return f"test-sub-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def mock() -> MagicMock:
    """Provide a mock for tracking calls."""
    from unittest.mock import MagicMock

    return MagicMock()


@pytest.fixture
def broker_factory(project_id: str) -> Callable[..., PubSubBroker]:
    """Factory fixture for creating brokers."""

    def _create(**kwargs: Any) -> PubSubBroker:
        return PubSubBroker(project_id=project_id, **kwargs)

    return _create


@pytest.fixture
def router_factory() -> Callable[..., PubSubRouter]:
    """Factory fixture for creating routers."""

    def _create(**kwargs: Any) -> PubSubRouter:
        return PubSubRouter(**kwargs)

    return _create


def callstack_matches(
    callstack: BaseMiddleware,
    expected_output: Sequence[type[BaseMiddleware]],
) -> bool:
    """Verify that the callstack matches the expected order of middlewares/commands.

    Args:
        callstack: The callstack to verify.
        expected_output: The expected order of middlewares/commands.

    Returns:
        True if the callstack matches the expected output, False otherwise.
    """
    callstack_collection = []

    next_call = callstack
    while next_call is not None:
        callstack_collection.append(next_call)
        next_call = getattr(next_call, "next_call", None)

    assert len(callstack_collection) == len(expected_output), "The callstacks do not match in size"

    for i in range(len(callstack_collection)):
        existing_middleware = type(callstack_collection[i])
        expected_middlewares = expected_output[i]
        assert existing_middleware == expected_middlewares, (
            f"The callstack on {i} is {existing_middleware} but should be {expected_middlewares}"
        )

    return True
