import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import HandleMessageCommand, PublishMessageCommand
from fastpubsub.router import PubSubRouter


@pytest.fixture
def broker() -> PubSubBroker:
    return PubSubBroker(project_id="abc")


@pytest.fixture
def router_a() -> PubSubRouter:
    return PubSubRouter(prefix="a")


@pytest.fixture
def router_b():
    return PubSubRouter(prefix="b")


@pytest.fixture
def first_middleware() -> type[BaseMiddleware]:
    class FirstMiddleware(BaseMiddleware): ...

    return FirstMiddleware


@pytest.fixture
def second_middleware() -> type[BaseMiddleware]:
    class SecondMiddleware(BaseMiddleware): ...

    return SecondMiddleware


@pytest.fixture
def third_middleware() -> type[BaseMiddleware]:
    class ThirdMiddleware(BaseMiddleware): ...

    return ThirdMiddleware


@pytest.fixture
def final_middleware() -> type[BaseMiddleware]:
    class FinalMiddleware(BaseMiddleware): ...

    return FinalMiddleware


def callstack_matches(
    callstack: BaseMiddleware | PublishMessageCommand | HandleMessageCommand,
    expected_output: list[type[BaseMiddleware] | type[PublishMessageCommand]],
) -> bool:
    next_call = callstack
    while next_call is not None:
        if not isinstance(next_call, expected_output[0]):
            return False

        next_call = getattr(next_call, "next_call", None)
        expected_output.pop(0)

    if len(expected_output):
        return False

    return True
