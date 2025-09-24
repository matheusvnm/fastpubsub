import pytest

from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand


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


def check_callstack(callstack: BaseMiddleware | PublishMessageCommand,
                    expected_output: list[type[BaseMiddleware] | type[PublishMessageCommand]]):
    next_call = callstack
    while next_call is not None:
        assert isinstance(next_call, expected_output[0])
        next_call = getattr(next_call, "next_call", None)
        expected_output.pop(0)
