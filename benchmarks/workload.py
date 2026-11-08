"""Benchmark scenarios, handler workloads and metric recording."""

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field

THREAD_LIMIT = 40
MAX_MESSAGES = 1000
ACK_DEADLINE_SECONDS = 60
SENT_AT_ATTRIBUTE = "sent_at_ns"
CPU_WORK_INPUT = b"fastpubsub-benchmark" * 4

TEST_MESSAGE = {
    "name": "John",
    "age": 39,
    "fullname": "LongString" * 8,
    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
}


@dataclass(frozen=True)
class Scenario:
    """The handler workload and the load shape of one benchmark run.

    Attributes:
        name: The scenario identifier.
        description: A short text for the report.
        in_flight_messages: The number of messages that the echo loop keeps
            in flight.
        payload_size: The message size in bytes. Zero selects the JSON test
            message.
        cpu_milliseconds: The CPU work for each message, in milliseconds.
        wait_milliseconds: The wait for each message, in milliseconds.
    """

    name: str
    description: str
    in_flight_messages: int = 100
    payload_size: int = 0
    cpu_milliseconds: float = 0.0
    wait_milliseconds: float = 0.0

    @property
    def wait_seconds(self) -> float:
        """The wait for each message, in seconds."""
        return self.wait_milliseconds / 1000

    def build_payload(self) -> bytes:
        """Build the payload of the initial messages.

        Returns:
            The payload bytes.
        """
        if self.payload_size == 0:
            return json.dumps(TEST_MESSAGE).encode()
        return b"x" * self.payload_size


def build_scenarios() -> dict[str, Scenario]:
    """Build all benchmark scenarios.

    Returns:
        The scenarios by name.
    """
    scenarios = [
        Scenario("echo", "Receive and publish again, no handler work"),
        Scenario(
            "echo-payload-10kb",
            "Echo with a 10 KB payload",
            payload_size=10 * 1024,
        ),
        Scenario(
            "cpu-only",
            "1 ms of CPU work, no wait",
            in_flight_messages=MAX_MESSAGES,
            cpu_milliseconds=1.0,
        ),
    ]
    for wait_milliseconds in (10, 50, 100):
        scenarios.append(
            Scenario(
                f"wait-only-{wait_milliseconds}ms",
                f"{wait_milliseconds} ms wait, no CPU work",
                in_flight_messages=MAX_MESSAGES,
                wait_milliseconds=wait_milliseconds,
            )
        )
        scenarios.append(
            Scenario(
                f"cpu-and-wait-{wait_milliseconds}ms",
                f"1 ms of CPU work, then a {wait_milliseconds} ms wait",
                in_flight_messages=MAX_MESSAGES,
                cpu_milliseconds=1.0,
                wait_milliseconds=wait_milliseconds,
            )
        )
    return {scenario.name: scenario for scenario in scenarios}


SCENARIOS = build_scenarios()


def run_cpu_work(iterations: int) -> None:
    """Run a fixed amount of pure Python CPU work.

    Args:
        iterations: The number of hash rounds.
    """
    digest = CPU_WORK_INPUT
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()


def calibrate_cpu_iterations(target_milliseconds: float = 1.0) -> int:
    """Find the number of hash rounds that takes the target time.

    Args:
        target_milliseconds: The target CPU time, in milliseconds.

    Returns:
        The number of hash rounds.
    """
    sample_iterations = 100_000
    best_seconds = float("inf")
    for _ in range(5):
        start = time.perf_counter()
        run_cpu_work(sample_iterations)
        best_seconds = min(best_seconds, time.perf_counter() - start)

    seconds_per_iteration = best_seconds / sample_iterations
    return max(1, round(target_milliseconds / 1000 / seconds_per_iteration))


def build_attributes() -> dict[str, str]:
    """Build the message attributes with the publish time.

    Returns:
        The message attributes.
    """
    return {SENT_AT_ATTRIBUTE: str(time.time_ns())}


@dataclass
class Recorder:
    """Thread-safe recorder of the received messages.

    The recorder keeps the message ids after a reset, so it can find
    redeliveries of messages that arrived during the warm-up.
    """

    events: int = 0
    redeliveries: int = 0
    latencies_ns: list[int] = field(default_factory=list)
    _seen_message_ids: set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, message_id: str, attributes: dict[str, str]) -> None:
        """Record one received message.

        Args:
            message_id: The message id.
            attributes: The message attributes.
        """
        received_at_ns = time.time_ns()
        sent_at = attributes.get(SENT_AT_ATTRIBUTE)
        with self._lock:
            self.events += 1
            if message_id in self._seen_message_ids:
                self.redeliveries += 1
            else:
                self._seen_message_ids.add(message_id)
            if sent_at is not None:
                self.latencies_ns.append(received_at_ns - int(sent_at))

    def reset(self) -> None:
        """Clear the counters at the end of the warm-up."""
        with self._lock:
            self.events = 0
            self.redeliveries = 0
            self.latencies_ns = []

    def read(self) -> tuple[int, int, list[int]]:
        """Read the counters.

        Returns:
            The events, the redeliveries and the latencies.
        """
        with self._lock:
            return self.events, self.redeliveries, list(self.latencies_ns)
