from contextlib import contextmanager

import pytest

from docs.snippets.observability.e1_01_logging_basics import broker, logger
from fastpubsub.testing import PubSubTestClient


class TestObservabilityLoggingBasics:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_basic_logger_subscriber_processes_message_without_errors(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(topic="tasks", data={"task": "send-email"})
            processed_results = client.get_results()

        assert len(processed_results) == 1
        processed_result = next(iter(processed_results))

        assert processed_result.error is None
        assert processed_result.message.topic_name == "tasks"
        assert processed_result.message.subscriber_name == "handle_task"

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_user_task_handler_context_and_logging_behavior(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        contextualize_calls: list[dict[str, str | None]] = []

        @contextmanager
        def _contextualize(**kwargs: str | None):
            contextualize_calls.append(kwargs)
            yield

        monkeypatch.setattr(logger, "contextualize", _contextualize)

        async with PubSubTestClient(broker) as client:
            attributes = {"user_id": "user-42"}
            await client.publish(
                topic="user-tasks",
                data={"task": "buy_food"},
                attributes=attributes,
            )

            await client.publish(
                topic="tasks",
                data={"task": "try_cooking", "result": "failed"},
                attributes=attributes,
            )

            await client.publish(
                topic="orders",
                data={"order": "pizza!"},
                attributes=attributes,
            )

            processed_results = client.get_results()

        assert len(processed_results) == 3
        iterator = iter(processed_results)

        first_message = next(iterator)
        assert first_message.error is None
        assert first_message.message.subscriber_name == "handle_user_task"

        second_message = next(iterator)
        assert second_message.error is None
        assert second_message.message.subscriber_name == "handle_task"

        third_message = next(iterator)
        assert third_message.error is None
        assert third_message.message.subscriber_name == "handle_order"

        assert contextualize_calls == [{"user_id": "user-42"}]
