from unittest.mock import AsyncMock

import pytest

from docs.snippets.advanced import e1_04_ordering as snippet
from fastpubsub.testing import PubSubTestClient


class TestAdvancedOrdering:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_ordered_publish_sets_ordering_key_and_attributes(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await snippet.user_action()
            published_messages = client.get_published_messages()
            publish_calls = client._mock_client.publish.await_args_list

        assert len(published_messages) == 2
        assert published_messages[0].attributes == {"user_id": "user-123"}
        assert published_messages[1].attributes == {"user_id": "user-123"}
        assert publish_calls[0].kwargs["ordering_key"] == "user-123"
        assert publish_calls[1].kwargs["ordering_key"] == "user-123"

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_missing_user_id_uses_fallback_unknown_user(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        update_user_state = AsyncMock()
        monkeypatch.setattr(snippet, "update_user_state", update_user_state)

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="user-events", data={"action": "ping"})

        update_user_state.assert_awaited_once()
        assert update_user_state.await_args.args[0] == "unknown-user"

    @pytest.mark.docs
    def test_ordering_configuration_is_enabled_for_ordered_subscribers(self) -> None:
        subscribers = snippet.broker.router._get_subscribers()
        ordered = subscribers["user-events-ordered"]
        with_dlt = subscribers["ordered-processor"]

        assert ordered.delivery_policy.enable_message_ordering is True
        assert with_dlt.delivery_policy.enable_message_ordering is True
        assert with_dlt.dead_letter_policy is not None
        assert with_dlt.dead_letter_policy.topic_name == "events-dlq"
