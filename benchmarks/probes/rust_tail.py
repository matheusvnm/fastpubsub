"""Split the Rust echo latency into the publish part and the delivery part.

The probe keeps 100 messages in flight with the Rust core, as the echo
benchmark does. It records for each message the publish start, the
publish confirmation and the receive time. It also reports the gaps in
which no message arrives.

Environment variables:
    BENCHMARK_PROJECT_ID: The Google Cloud project.
    MAX_MESSAGES: The client limit of the subscription. The server gets
        twice this limit. The default is 1000.
    ACK_AFTER_PUBLISH: ``1`` acknowledges a message after its echo
        publish is confirmed, as fastpubsub does. The default
        acknowledges at once.
"""

import asyncio
import json
import os
import time
import uuid
from collections import Counter

from fastpubsub_core import (
    AckHandle,
    AdminClient,
    Bridge,
    Publisher,
    Subscription,
)

from fastpubsub._internal.event_loop_bridge import EventLoopBridge

IN_FLIGHT = 100
WARMUP_SECONDS = 10
MEASURE_SECONDS = 30
SLOW_MILLISECONDS = 500
PAYLOAD = json.dumps({"name": "John", "fullname": "LongString" * 8}).encode()


def percentile(values: list[float], rank: float) -> float:
    """Return the value at a percentile rank, or 0 for no values."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(rank / 100 * len(ordered)))]


def describe(name: str, values: list[float]) -> str:
    """Format the percentiles of one latency part."""
    return (
        f"{name:9} p50={percentile(values, 50):7.1f} "
        f"p90={percentile(values, 90):7.1f} "
        f"p99={percentile(values, 99):7.1f} "
        f"max={max(values, default=0):7.1f} ms"
    )


async def run_probe() -> None:
    """Run the echo loop and print the latency split."""
    loop = asyncio.get_running_loop()
    project = os.environ["BENCHMARK_PROJECT_ID"]
    max_messages = int(os.getenv("MAX_MESSAGES", "1000"))
    is_ack_after_publish = os.getenv("ACK_AFTER_PUBLISH") == "1"
    suffix = uuid.uuid4().hex[:8]
    topic = f"projects/{project}/topics/probe-{suffix}"
    subscription_path = f"projects/{project}/subscriptions/probe-{suffix}"
    admin = AdminClient(None)
    admin.create_topic(topic)
    admin.create_subscription(
        subscription_path, topic, {"ack_deadline_seconds": 60}
    )
    bridge = Bridge(
        EventLoopBridge.MAX_BATCH_SIZE,
        loop.call_soon_threadsafe,
        EventLoopBridge.run_callbacks,
    )
    publisher = Publisher(topic, bridge, EventLoopBridge.resolve_futures, None)
    confirmed_at: dict[str, float] = {}
    records: list[tuple[str, float, float]] = []
    measure_start: list[float] = []
    is_stopping = [False]

    def publish(ack: AckHandle | None = None) -> None:
        future = loop.create_future()
        publisher.publish(PAYLOAD, {"sent": repr(time.time())}, future, None)

        def confirm(done: asyncio.Future[str]) -> None:
            if not done.cancelled() and done.exception() is None:
                confirmed_at[done.result()] = time.time()
            if ack is not None:
                ack.ack()

        future.add_done_callback(confirm)

    def dispatch(
        batch: list[tuple[str, bytes, dict[str, str], int, AckHandle]],
    ) -> None:
        now = time.time()
        for message_id, _data, attributes, _attempt, ack in batch:
            sent = float(attributes["sent"])
            if measure_start and sent >= measure_start[0]:
                records.append((message_id, sent, now))
            if is_stopping[0]:
                ack.ack()
            elif is_ack_after_publish:
                publish(ack)
            else:
                ack.ack()
                publish()

    subscription = Subscription(
        subscription_path, max_messages, bridge, dispatch, None
    )
    try:
        await asyncio.sleep(2)
        for _ in range(IN_FLIGHT):
            publish()
        await asyncio.sleep(WARMUP_SECONDS)
        measure_start.append(time.time())
        await asyncio.sleep(MEASURE_SECONDS)
        is_stopping[0] = True
        await asyncio.sleep(3)
    finally:
        subscription.shutdown(10)
        publisher.flush()
        admin.delete_subscription(subscription_path)
        admin.delete_topic(topic)

    print_report(records, confirmed_at, measure_start[0], max_messages)


def print_report(
    records: list[tuple[str, float, float]],
    confirmed_at: dict[str, float],
    measure_start: float,
    max_messages: int,
) -> None:
    """Print the latency split and the arrival gaps."""
    total, publish_part, delivery_part = [], [], []
    for message_id, sent, received in records:
        total.append((received - sent) * 1000)
        confirmed = confirmed_at.get(message_id)
        if confirmed is not None:
            publish_part.append((confirmed - sent) * 1000)
            delivery_part.append((received - confirmed) * 1000)
    arrivals = sorted(received for _id, _sent, received in records)
    gaps = sorted(
        (later - earlier) * 1000
        for earlier, later in zip(arrivals, arrivals[1:], strict=False)
    )
    windows = Counter(
        int((received - measure_start) * 4) for *_, received in records
    )
    quiet_windows = sum(
        1 for window in range(MEASURE_SECONDS * 4) if windows[window] < 20
    )
    slow = sum(1 for value in total if value > SLOW_MILLISECONDS)
    print(f"client limit: {max_messages}")
    print(f"events/s: {len(records) / MEASURE_SECONDS:.0f}")
    print(describe("total", total))
    print(describe("publish", publish_part))
    print(describe("delivery", delivery_part))
    print(f"slower than {SLOW_MILLISECONDS} ms: {slow} of {len(total)}")
    print(
        f"largest gaps without arrivals: {[round(gap) for gap in gaps[-5:]]}"
    )
    print(f"250 ms windows with under 20 arrivals: {quiet_windows} of 120")


if __name__ == "__main__":
    asyncio.run(run_probe())
