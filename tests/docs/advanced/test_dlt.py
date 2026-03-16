import pytest

from docs.snippets.advanced.e1_02_dlt import broker
from fastpubsub.testing import PubSubTestClient

_SNIPPET = "docs.snippets.advanced.e1_02_dlt"


class TestAdvancedDlt:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_failing_subscriber_surfaces_error_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _fail(_: bytes) -> None:
            raise ValueError("invalid payload")

        monkeypatch.setattr(f"{_SNIPPET}.process_payment", _fail)

        async with PubSubTestClient(broker) as client:
            await client.publish(topic="orders", data={"order_id": "ord-1"})
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, ValueError)
