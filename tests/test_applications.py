import json
import os
from contextlib import asynccontextmanager
from types import FunctionType
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.responses import JSONResponse

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker


@pytest.fixture
def mock_broker() -> MagicMock:
    os.environ["FASTPUBSUB_APM_PROVIDER"] = "noop"
    broker = MagicMock(spec=PubSubBroker)
    broker.start = AsyncMock()
    broker.shutdown = AsyncMock()
    return broker


class TestApplicationLifecycle:
    @pytest.mark.asyncio
    async def test_application_hooks_are_called_on_constructor(self, mock_broker: MagicMock):
        on_startup_action = AsyncMock(spec=FunctionType)
        after_startup_action = AsyncMock(spec=FunctionType)
        on_shutdown_action = AsyncMock(spec=FunctionType)
        after_shutdown_action = AsyncMock(spec=FunctionType)

        app = FastPubSub(
            broker=mock_broker,
            on_startup=[on_startup_action],
            after_startup=[after_startup_action],
            on_shutdown=[on_shutdown_action],
            after_shutdown=[after_shutdown_action],
        )

        async with app.run(app):
            on_startup_action.assert_called_once()
            after_startup_action.assert_called_once()
            mock_broker.start.assert_called_once()

        on_shutdown_action.assert_called_once()
        after_shutdown_action.assert_called_once()
        mock_broker.shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_hooks_are_called(self, mock_broker: MagicMock):
        app = FastPubSub(broker=mock_broker)

        on_startup_mock = AsyncMock(spec=FunctionType)
        after_startup_mock = AsyncMock(spec=FunctionType)
        on_shutdown_mock = AsyncMock(spec=FunctionType)
        after_shutdown_mock = AsyncMock(spec=FunctionType)

        app.on_startup(on_startup_mock)
        app.after_startup(after_startup_mock)
        app.on_shutdown(on_shutdown_mock)
        app.after_shutdown(after_shutdown_mock)

        async with app.run(app):
            on_startup_mock.assert_called_once()
            after_startup_mock.assert_called_once()

        on_shutdown_mock.assert_called_once()
        after_shutdown_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_decorator_hooks_are_called_with_lifespan(self, mock_broker: MagicMock):
        start_lifepan_call = MagicMock()
        end_lifepan_call = MagicMock()

        @asynccontextmanager
        async def lifespan(_):
            start_lifepan_call()
            yield
            end_lifepan_call()

        on_startup_action = AsyncMock(spec=FunctionType)
        after_startup_action = AsyncMock(spec=FunctionType)
        on_shutdown_action = AsyncMock(spec=FunctionType)
        after_shutdown_action = AsyncMock(spec=FunctionType)

        app = FastPubSub(
            broker=mock_broker,
            on_startup=[on_startup_action],
            after_startup=[after_startup_action],
            on_shutdown=[on_shutdown_action],
            after_shutdown=[after_shutdown_action],
            lifespan=lifespan,
        )

        async with app.run(app):
            on_startup_action.assert_called_once()
            after_startup_action.assert_called_once()
            mock_broker.start.assert_called_once()

        start_lifepan_call.assert_called_once()
        end_lifepan_call.assert_called_once()
        on_shutdown_action.assert_called_once()
        after_shutdown_action.assert_called_once()
        mock_broker.shutdown.assert_called_once()


class TestApplicationProbes:
    @pytest.mark.parametrize(
        ["ready", "status_code"],
        [
            (True, 200),
            (False, 500),
        ],
    )
    @pytest.mark.asyncio
    async def test_ready_probe_reflect_broker_state(
        self, mock_broker: MagicMock, ready: bool, status_code: int
    ):
        mock_broker.ready.return_value = ready
        app = FastPubSub(broker=mock_broker)

        response = await app._get_readiness(None)
        assert isinstance(response, JSONResponse)
        assert response.status_code == status_code
        assert json.loads(response.body) == {"ready": ready}

    @pytest.mark.parametrize(
        ["alive", "status_code"],
        [
            (True, 200),
            (False, 500),
        ],
    )
    @pytest.mark.asyncio
    async def test_alive_probe_reflect_broker_state(
        self, mock_broker: MagicMock, alive: bool, status_code: int
    ):
        mock_broker.alive.return_value = alive
        app = FastPubSub(broker=mock_broker)

        response = await app._get_liveness(None)
        assert isinstance(response, JSONResponse)
        assert response.status_code == status_code
        assert json.loads(response.body) == {"alive": alive}
