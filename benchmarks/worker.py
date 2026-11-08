"""Run one benchmark case once, in its own process.

The worker prints one result line that starts with ``RESULT``, followed
by a JSON object. The orchestrator in ``benchmarks.bench`` reads it.
"""

import argparse
import asyncio
import json
import math
import resource
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

from benchmarks.cases import load_case
from benchmarks.resources import create_run_resources, delete_run_resources
from benchmarks.workload import SCENARIOS, Recorder

RESULT_PREFIX = "RESULT "


@dataclass
class RunResult:
    """The measurements of one benchmark run."""

    events: int
    elapsed_seconds: float
    cpu_seconds: float
    redeliveries: int
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    peak_rss_mb: float
    fastpubsub_version: str
    fastpubsub_location: str

    @property
    def events_per_second(self) -> float:
        """The number of events for each second."""
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.events / self.elapsed_seconds

    @property
    def cpu_microseconds_per_event(self) -> float:
        """The CPU time of the process for each event."""
        if self.events <= 0:
            return 0.0
        return self.cpu_seconds * 1_000_000 / self.events


def read_cpu_seconds(process: psutil.Process) -> float:
    """Read the user and system CPU time of all threads of a process.

    Args:
        process: The process to read.

    Returns:
        The CPU time in seconds.
    """
    cpu_times = process.cpu_times()
    return float(cpu_times.user + cpu_times.system)


def read_peak_rss_mb() -> float:
    """Read the peak resident memory of this process.

    Returns:
        The peak resident memory in megabytes.
    """
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    bytes_per_unit = 1 if sys.platform == "darwin" else 1024
    return peak_rss * bytes_per_unit / (1024 * 1024)


def calculate_percentile_ms(
    sorted_latencies_ns: list[int], rank: float
) -> float:
    """Calculate a latency percentile with the nearest-rank method.

    Args:
        sorted_latencies_ns: The latencies in nanoseconds, in ascending order.
        rank: The percentile rank, from 0 to 100.

    Returns:
        The percentile in milliseconds, or 0 when there are no latencies.
    """
    if not sorted_latencies_ns:
        return 0.0
    index = max(0, math.ceil(rank / 100 * len(sorted_latencies_ns)) - 1)
    return sorted_latencies_ns[index] / 1_000_000


async def run_case(
    case_name: str,
    scenario_name: str,
    duration: float,
    warmup: float,
    cpu_iterations: int,
) -> RunResult:
    """Run one benchmark case once.

    Args:
        case_name: The benchmark case name.
        scenario_name: The scenario name.
        duration: The measured time in seconds.
        warmup: The time in seconds before the measurement starts.
        cpu_iterations: The hash rounds for 1 ms of CPU work.

    Returns:
        The measurements.
    """
    case_factory = load_case(case_name)
    resources = create_run_resources(case_name)
    recorder = Recorder()
    process = psutil.Process()

    try:
        case = case_factory(
            SCENARIOS[scenario_name], cpu_iterations, resources, recorder
        )
        async with case.run():
            await asyncio.sleep(warmup)
            recorder.reset()
            start_cpu_seconds = read_cpu_seconds(process)
            start_time = time.perf_counter()

            await asyncio.sleep(duration)

            events, redeliveries, latencies_ns = recorder.read()
            elapsed_seconds = time.perf_counter() - start_time
            cpu_seconds = read_cpu_seconds(process) - start_cpu_seconds
    finally:
        delete_run_resources(resources)

    import fastpubsub
    from fastpubsub.__about__ import __version__

    latencies_ns.sort()
    return RunResult(
        events=events,
        elapsed_seconds=elapsed_seconds,
        cpu_seconds=cpu_seconds,
        redeliveries=redeliveries,
        latency_p50_ms=calculate_percentile_ms(latencies_ns, 50),
        latency_p95_ms=calculate_percentile_ms(latencies_ns, 95),
        latency_p99_ms=calculate_percentile_ms(latencies_ns, 99),
        peak_rss_mb=read_peak_rss_mb(),
        fastpubsub_version=__version__,
        fastpubsub_location=str(Path(fastpubsub.__file__).parent.parent),
    )


def main() -> None:
    """Parse the arguments, run the case and print the result."""
    parser = argparse.ArgumentParser(description="Run one benchmark case")
    parser.add_argument("--case", required=True)
    parser.add_argument("--scenario", required=True, choices=list(SCENARIOS))
    parser.add_argument("--duration", type=float, required=True)
    parser.add_argument("--warmup", type=float, required=True)
    parser.add_argument("--cpu-iterations", type=int, required=True)
    parser.add_argument(
        "--fastpubsub-source",
        help="A folder that contains a different fastpubsub package",
    )
    args = parser.parse_args()

    if args.fastpubsub_source:
        sys.path.insert(0, str(Path(args.fastpubsub_source).resolve()))

    result = asyncio.run(
        run_case(
            case_name=args.case,
            scenario_name=args.scenario,
            duration=args.duration,
            warmup=args.warmup,
            cpu_iterations=args.cpu_iterations,
        )
    )
    print(RESULT_PREFIX + json.dumps(asdict(result)), flush=True)


if __name__ == "__main__":
    main()
