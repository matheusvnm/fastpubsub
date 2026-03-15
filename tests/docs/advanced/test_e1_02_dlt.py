import pytest

from docs.snippets.advanced import e1_02_dlt as snippet
from fastpubsub.testing import PubSubTestClient


class TestAdvancedDlt:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_failing_subscriber_surfaces_error_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def _fail(_: bytes) -> None:
            raise ValueError("invalid payload")

        monkeypatch.setattr(snippet, "process_payment", _fail)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="orders", data={"order_id": "ord-1"})
            results = client.get_results()

        assert len(results) == 1
        assert isinstance(results[0].error, ValueError)

    @pytest.mark.docs
    def test_dlt_and_backoff_configuration_values_are_preserved(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()
        order_processor = subscribers["order-processor"]
        api_caller = subscribers["api-caller"]

        assert order_processor.dead_letter_policy is not None
        assert order_processor.dead_letter_policy.topic_name == "orders-dlq"
        assert order_processor.dead_letter_policy.max_delivery_attempts == 5

        assert api_caller.dead_letter_policy is not None
        assert api_caller.dead_letter_policy.topic_name == "api-requests-dlq"
        assert api_caller.dead_letter_policy.max_delivery_attempts == 10
        assert api_caller.retry_policy.min_backoff_delay_secs == 10
        assert api_caller.retry_policy.max_backoff_delay_secs == 600
