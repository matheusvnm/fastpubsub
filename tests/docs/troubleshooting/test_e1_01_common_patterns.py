import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from docs.snippets.troubleshooting import e1_01_common_patterns as snippet
from fastpubsub.exceptions import Drop
from fastpubsub.testing import PubSubTestClient


class TestTroubleshootingCommonPatterns:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_idempotent_handler_processes_and_skips_duplicates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        do_work = AsyncMock()
        processed_keys: set[str] = set()

        redis_exists = AsyncMock(side_effect=lambda key: key in processed_keys)

        async def _set(key: str, value: str, ex: int = 0) -> None:
            assert value == "1"
            assert ex == 86400
            processed_keys.add(key)

        redis_set = AsyncMock(side_effect=_set)

        monkeypatch.setattr(snippet, "do_work", do_work)
        monkeypatch.setattr(
            snippet,
            "redis",
            SimpleNamespace(exists=redis_exists, set=redis_set),
        )

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(
                topic="idempotent-events",
                data={"value": "ok"},
                attributes={"event_id": "evt-1"},
            )
            await client.publish(
                topic="idempotent-events",
                data={"value": "ok"},
                attributes={"event_id": "evt-1"},
            )
            results = client.get_results()

        assert len(results) == 2
        assert all(result.error is None for result in results)
        assert do_work.await_count == 1
        do_work_args = do_work.await_args
        assert do_work_args is not None
        assert json.loads(do_work_args.args[0]) == {"value": "ok"}
        assert redis_exists.await_count == 2
        redis_set.assert_awaited_once_with("processed:evt-1", "1", ex=86400)
        assert "processed:evt-1" in processed_keys

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_validated_handler_drops_invalid_message(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="validated-events", data={"wrong": "shape"})
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, Drop)
        assert "Invalid message format" in str(results[0].error)
