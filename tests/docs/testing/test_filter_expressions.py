import pytest

from docs.snippets.testing.e1_03_filter_expressions import broker
from fastpubsub.testing import PubSubTestClient


class TestTestingFilterExpressions:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_filter_expression_routes_matching_message(self) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(
                {"order_id": "ord-1"},
                topic="events",
                attributes={"event_type": "order"},
            )
            await client.publish(
                {"user_id": "usr-1"},
                topic="events",
                attributes={"event_type": "user"},
            )
            processed_results = client.get_results()

        assert len(processed_results) == 1
        processed_result = next(iter(processed_results))

        assert "ord-1" in processed_result.return_value
        assert processed_result.message.subscriber_name == "handle_orders"
        assert processed_result.message.attributes["event_type"] == "order"
