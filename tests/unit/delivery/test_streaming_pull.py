"""Unit tests for PubSubStreamingPullTask."""

import asyncio
from unittest.mock import MagicMock

import pytest

from fastpubsub._internal.delivery.streaming_pull import (
    PubSubStreamingPullTask,
)


class FakeAckHandle:
    """Count the acks and nacks of one message."""

    def __init__(self) -> None:
        self.ack_count = 0
        self.nack_count = 0

    def ack(self) -> None:
        self.ack_count += 1

    def nack(self) -> None:
        self.nack_count += 1


@pytest.fixture
def release_handlers() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture
def subscriber(release_handlers: asyncio.Event) -> MagicMock:
    async def on_message(message: object) -> None:
        await release_handlers.wait()

    subscriber = MagicMock()
    subscriber.project_id = "test-project"
    subscriber.topic_name = "test-topic"
    subscriber.name = "test-subscriber"
    subscriber.control_flow_policy.max_messages = 2
    subscriber.callstack.on_message = on_message
    return subscriber


def build_batch(
    handles: list[FakeAckHandle],
) -> list[tuple[str, bytes, dict[str, str], int, FakeAckHandle]]:
    return [
        (f"message-{index}", b"data", {}, 0, handle)
        for index, handle in enumerate(handles)
    ]


async def test_runs_at_most_max_messages_handlers(
    subscriber: MagicMock, release_handlers: asyncio.Event
) -> None:
    task = PubSubStreamingPullTask(subscriber)
    handles = [FakeAckHandle() for _ in range(5)]

    task._dispatch(build_batch(handles))

    assert len(task.tasks) == 2
    assert len(task.waiting) == 3

    release_handlers.set()
    while task.tasks or task.waiting:
        await asyncio.sleep(0)

    assert [handle.ack_count for handle in handles] == [1, 1, 1, 1, 1]
    assert [handle.nack_count for handle in handles] == [0, 0, 0, 0, 0]


async def test_shutdown_nacks_waiting_and_late_messages(
    subscriber: MagicMock, release_handlers: asyncio.Event
) -> None:
    task = PubSubStreamingPullTask(subscriber)
    handles = [FakeAckHandle() for _ in range(4)]
    task._dispatch(build_batch(handles))

    asyncio.get_running_loop().call_later(0.01, release_handlers.set)
    await task.shutdown(timeout=1)
    late_handle = FakeAckHandle()
    task._dispatch(build_batch([late_handle]))

    assert [handle.ack_count for handle in handles] == [1, 1, 0, 0]
    assert [handle.nack_count for handle in handles] == [0, 0, 1, 1]
    assert late_handle.nack_count == 1
    assert not task.tasks
