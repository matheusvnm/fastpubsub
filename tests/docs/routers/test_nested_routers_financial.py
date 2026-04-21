import pytest

from docs.snippets.routers.e1_02_nested_routers_financial import (
    broker,
    publish_test_messages,
)
from fastpubsub.testing import PubSubTestClient


class TestRoutersNestedRoutersFinancial:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_nested_router_handlers_receive_messages_for_each_domain(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await publish_test_messages()

            published_messages = client.get_published_messages()
            processed_results = client.get_results()
            subscribers_alias = set(broker.router.get_subscribers())

        assert len(subscribers_alias) == 3
        assert len(published_messages) == 3
        assert len(processed_results) == 3

        topics = [result.message.topic_name for result in processed_results]
        subscribers_names = {
            result.message.subscriber_name for result in processed_results
        }

        assert topics == [
            "core-topic",
            "banking-topic",
            "finance-topic",
        ]

        assert subscribers_names == {
            "handle_message_core",
            "handle_message_banking",
            "handle_message_finance",
        }

        assert subscribers_alias == {
            "core.core_handler",
            "core.banking.banking_handler",
            "core.finance.finance_handler",
        }

        assert all(result.error is None for result in processed_results)
