from unittest.mock import AsyncMock

import httpx
import pytest

from docs.snippets.basic_usage.e7_02_lifecycle_retry import broker
from fastpubsub.exceptions import Retry
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.basic_usage.e7_02_lifecycle_retry"


class _SuccessClient:
    def __init__(self, post_mock: AsyncMock) -> None:
        self._post_mock = post_mock

    async def __aenter__(self) -> "_SuccessClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def post(self, url: str) -> None:
        await self._post_mock(url)


class _TimeoutClient:
    async def __aenter__(self) -> "_TimeoutClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def post(self, url: str) -> None:
        raise httpx.TimeoutException(f"timeout for {url}")


class TestLifecycleRetry:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_timeout_error_raises_retry_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(f"{_SNIPPET}.httpx.AsyncClient", _TimeoutClient)

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="orders", data={"order_id": "ord-1"})
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, Retry)
        assert str(results[0].error) == "Downstream service timed out."

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_successful_downstream_call_does_not_raise_retry(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        post_mock = AsyncMock()
        monkeypatch.setattr(
            f"{_SNIPPET}.httpx.AsyncClient", lambda: _SuccessClient(post_mock)
        )

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="orders", data={"order_id": "ord-200"})
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        post_mock.assert_awaited_once_with(
            "https://downstream.service/process/ord-200"
        )
