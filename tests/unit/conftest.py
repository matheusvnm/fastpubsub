"""Unit test fixtures for FastPubSub."""

from __future__ import annotations

import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
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

    class FirstMiddleware(BaseMiddleware):
        def __init__(
            self, next_call: BaseMiddleware, arg_1: str = "", arg_2: str = ""
        ):
            super().__init__(next_call)
            self.arg_1 = arg_1
            self.arg_2 = arg_2

    return FirstMiddleware


@pytest.fixture
def second_middleware() -> type[BaseMiddleware]:
    """Create a SecondMiddleware class for testing."""

    class SecondMiddleware(BaseMiddleware):
        def __init__(
            self, next_call: BaseMiddleware, arg_1: str = "", arg_2: str = ""
        ):
            super().__init__(next_call)
            self.arg_1 = arg_1
            self.arg_2 = arg_2

    return SecondMiddleware


@pytest.fixture
def third_middleware() -> type[BaseMiddleware]:
    """Create a ThirdMiddleware class for testing."""

    class ThirdMiddleware(BaseMiddleware):
        def __init__(
            self, next_call: BaseMiddleware, arg_1: str = "", arg_2: str = ""
        ):
            super().__init__(next_call)
            self.arg_1 = arg_1
            self.arg_2 = arg_2

    return ThirdMiddleware


@pytest.fixture
def final_middleware() -> type[BaseMiddleware]:
    """Create a FinalMiddleware class for testing."""

    class FinalMiddleware(BaseMiddleware):
        def __init__(
            self, next_call: BaseMiddleware, arg_1: str = "", arg_2: str = ""
        ):
            super().__init__(next_call)
            self.arg_1 = arg_1
            self.arg_2 = arg_2

    return FinalMiddleware


@pytest.fixture
def publisher(broker: PubSubBroker) -> Publisher:
    """Create a Publisher for testing."""
    return broker.publisher(topic_name="cba")
