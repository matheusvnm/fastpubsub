import pytest

from docs.snippets.basic_usage.e7_01_lifecycle_drop import broker
from fastpubsub.exceptions import Drop
from fastpubsub.testing import PubSubTestClient


class TestLifecycleDrop:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_v1_schema_is_dropped(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                topic="events",
                data={"event": "legacy"},
                attributes={"schema_version": "v1"},
            )
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, Drop)
        assert str(results[0].error) == "Schema version v1 is deprecated."

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_v2_schema_is_processed_without_errors(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                topic="events",
                data={"event": "current"},
                attributes={"schema_version": "v2"},
            )
            results = client.get_results()

        assert len(results) == 1
        assert results[0].error is None
        assert results[0].message.topic_name == "events"
