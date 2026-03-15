from unittest.mock import AsyncMock

import pytest

from docs.snippets.basic_usage import e7_03_lifecycle_unhandled as snippet
from fastpubsub.testing import PubSubTestClient


class TestLifecycleUnhandled:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_valid_payload_invokes_process_function(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = AsyncMock()
        monkeypatch.setattr(snippet, "process", process)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"status": "ok"})
            results = client.get_results()

        process.assert_awaited_once_with({"status": "ok"})
        assert len(results) == 1
        assert results[0].error is None

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_unhandled_exception_is_recorded_in_processing_results(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process = AsyncMock(side_effect=ValueError("bad payload"))
        monkeypatch.setattr(snippet, "process", process)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"status": "bad"})
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, ValueError)
        assert str(results[0].error) == "bad payload"
