import pytest

from docs.snippets.basic_usage import e6_01_lifespan_hooks as snippet
from fastpubsub.testing import PubSubTestClient


class TestLifespanHooks:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_after_startup_hook_publishes_system_online_message(self) -> None:
        async with PubSubTestClient(snippet.broker) as client:
            await snippet.announce_startup()
            published_messages = client.get_published_messages()
            results = client.get_results()

        assert len(published_messages) == 1
        assert published_messages[0].topic_name == "system-logs"
        assert published_messages[0].data == b'{"status":"online"}'
        assert len(results) == 1
        assert results[0].message.subscriber_name == "handle_system_log"
        assert results[0].error is None

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_shutdown_hook_sets_application_state_flag(self) -> None:
        await snippet.prepare_for_shutdown()

        assert snippet.app.state.is_shutting_down is True
        assert [hook.__name__ for hook in snippet.app._on_startup] == ["setup_database"]
        assert [hook.__name__ for hook in snippet.app._after_startup] == ["announce_startup"]
        assert [hook.__name__ for hook in snippet.app._on_shutdown] == ["prepare_for_shutdown"]
        assert [hook.__name__ for hook in snippet.app._after_shutdown] == ["cleanup_database"]
