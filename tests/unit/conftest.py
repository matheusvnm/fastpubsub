"""Unit test fixtures for FastPubSub."""

from __future__ import annotations

import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter


@pytest.fixture
def broker() -> PubSubBroker:
    """Create a PubSubBroker for testing."""
    return PubSubBroker(project_id="abc")


@pytest.fixture
def router_a() -> PubSubRouter:
    """Create a PubSubRouter with prefix 'a' for testing."""
    return PubSubRouter(prefix="a")


@pytest.fixture
def router_b() -> PubSubRouter:
    """Create a PubSubRouter with prefix 'b' for testing."""
    return PubSubRouter(prefix="b")


@pytest.fixture
def first_middleware() -> type[BaseMiddleware]:
    """Create a FirstMiddleware class for testing."""

    class FirstMiddleware(BaseMiddleware): ...

    return FirstMiddleware


@pytest.fixture
def second_middleware() -> type[BaseMiddleware]:
    """Create a SecondMiddleware class for testing."""

    class SecondMiddleware(BaseMiddleware): ...

    return SecondMiddleware


@pytest.fixture
def third_middleware() -> type[BaseMiddleware]:
    """Create a ThirdMiddleware class for testing."""

    class ThirdMiddleware(BaseMiddleware): ...

    return ThirdMiddleware


@pytest.fixture
def final_middleware() -> type[BaseMiddleware]:
    """Create a FinalMiddleware class for testing."""

    class FinalMiddleware(BaseMiddleware): ...

    return FinalMiddleware


@pytest.fixture
def publisher(broker: PubSubBroker) -> Publisher:
    """Create a Publisher for testing."""
    return broker.publisher(topic_name="cba")
