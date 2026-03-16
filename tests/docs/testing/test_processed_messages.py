import pytest

from docs.snippets.testing.e1_05_test_processed_messages import broker
from fastpubsub.testing import PubSubTestClient


class TestTestingProcessedMessages:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_processed_result_is_captured_for_valid_order(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                {"id": "order-1", "amount": 100},
                topic="orders",
            )
            processed_results = client.get_results()

        assert len(processed_results) == 1
        processed_result = next(iter(processed_results))

        assert processed_result.error is None
        assert processed_result.return_value == "processed-order-1"
        assert processed_result.message.topic_name == "orders"

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_processed_result_records_error_for_invalid_order(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                {"id": "order-2", "amount": -5},
                topic="orders",
            )
            processed_results = client.get_results()

        assert len(processed_results) == 1
        processed_result = next(iter(processed_results))

        assert processed_result.return_value is None
        assert isinstance(processed_result.error, ValueError)
