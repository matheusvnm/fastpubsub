"""Core benchmark framework for FastPubSub performance measurement.

This module provides the infrastructure to measure Events Per Second (EPS)
for different test cases, comparing FastPubSub performance against baseline.

Usage:
    python -m benchmarks.bench --case basic --duration 60
    python -m benchmarks.bench --case raw_pubsub --duration 60
    python -m benchmarks.bench --all --duration 60
"""

import argparse
import asyncio
import csv
import platform
import sys
import time
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import psutil

from fastpubsub.__about__ import __version__


class BenchmarkCase(Protocol):
    """Protocol defining the interface for all benchmark test cases.

    All benchmark cases must implement:
        - EVENTS_PROCESSED: Counter for processed messages
        - case_name: Identifier for the test case
        - description: Human-readable description
        - start(): Async context manager that runs the benchmark
    """

    EVENTS_PROCESSED: int
    case_name: str
    description: str

    def start(self) -> AbstractAsyncContextManager[float]:
        """Start the benchmark and yield the start time.

        Returns:
            Async context manager yielding the start timestamp.
        """
        ...


@dataclass
class MeasureResult:
    """Container for benchmark measurement results.

    Attributes:
        total_events: Total number of events processed.
        elapsed_time: Time elapsed in seconds.
    """

    total_events: int
    elapsed_time: float

    @property
    def eps(self) -> float:
        """Calculate Events Per Second.

        Returns:
            float: Events per second (total_events / elapsed_time).
        """
        if self.elapsed_time == 0:
            return 0.0
        return self.total_events / self.elapsed_time


async def measure(case: BenchmarkCase, measure_time: int) -> AsyncGenerator[MeasureResult, None]:
    """Run benchmark and yield results every second.

    Args:
        case: The benchmark case to run.
        measure_time: Duration to run the benchmark in seconds.

    Yields:
        MeasureResult: Current measurement snapshot every second.
    """
    async with case.start() as start_time:
        while (elapsed_time := (time.time() - start_time)) < measure_time:
            yield MeasureResult(case.EVENTS_PROCESSED, elapsed_time)
            await asyncio.sleep(1.0)

    yield MeasureResult(case.EVENTS_PROCESSED, time.time() - start_time)


async def run_benchmark(case: BenchmarkCase, measure_time: int) -> MeasureResult:
    """Execute a benchmark with real-time progress display.

    Args:
        case: The benchmark case to run.
        measure_time: Duration to run the benchmark in seconds.

    Returns:
        MeasureResult: Final measurement results.
    """
    result = MeasureResult(0, 0.0)

    async for result in measure(case, measure_time):
        # Clear line and print progress
        sys.stdout.write(
            f"\r[{case.case_name}] Events: {result.total_events:,}, "
            f"Time: {result.elapsed_time:.1f}s ({(measure_time - result.elapsed_time):.1f}s left), "
            f"EPS: {result.eps:,.2f}    "
        )
        sys.stdout.flush()

    # Print newline after progress
    print()
    return result


def save_results(
    case: BenchmarkCase,
    result: MeasureResult,
    results_file: Path,
) -> None:
    """Save benchmark results to CSV file.

    Args:
        case: The benchmark case that was run.
        result: The measurement results.
        results_file: Path to the CSV file.
    """
    file_exists = results_file.exists()
    mem = psutil.virtual_memory()

    with results_file.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")

        # Write header if file is new
        if not file_exists:
            writer.writerow(
                [
                    "FastPubSub Version",
                    "Case",
                    "Total Events",
                    "Elapsed Time",
                    "EPS",
                    "Timestamp",
                    "Python Version",
                    "Description",
                    "Host Memory",
                ]
            )

        writer.writerow(
            [
                __version__,
                case.case_name,
                result.total_events,
                f"{result.elapsed_time:.2f}",
                f"{result.eps:.2f}",
                datetime.now(tz=UTC).isoformat(),
                platform.python_version(),
                case.description,
                f"{mem.total / (1024**3):.2f} GB",
            ]
        )


def print_results(cases_results: list[tuple[BenchmarkCase, MeasureResult]]) -> None:
    """Print formatted benchmark results table.

    Args:
        cases_results: List of (case, result) tuples.
    """
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    print(f"{'Case':<15} | {'Events':>12} | {'EPS':>12} | Description")
    print("-" * 60)

    for case, result in cases_results:
        print(
            f"{case.case_name:<15} | {result.total_events:>12,} | "
            f"{result.eps:>12,.2f} | {case.description}"
        )

    print("-" * 60)

    # Calculate overhead if we have both cases
    if len(cases_results) == 2:
        raw_result = next((r for c, r in cases_results if c.case_name == "raw_pubsub"), None)
        basic_result = next((r for c, r in cases_results if c.case_name == "basic"), None)

        if raw_result and basic_result and raw_result.eps > 0:
            overhead = ((raw_result.eps - basic_result.eps) / raw_result.eps) * 100
            print(f"FastPubSub overhead: {overhead:.1f}%")
            print("=" * 60)


async def main() -> None:
    """Main entry point for the benchmark CLI."""
    parser = argparse.ArgumentParser(
        description="FastPubSub Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m benchmarks.bench --case basic --duration 60
    python -m benchmarks.bench --case raw_pubsub --duration 60
    python -m benchmarks.bench --all --duration 60
        """,
    )
    parser.add_argument(
        "--case",
        choices=["basic", "raw_pubsub"],
        default="basic",
        help="Benchmark case to run (default: basic)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all benchmark cases",
    )

    args = parser.parse_args()

    # Import cases here to avoid circular imports
    from benchmarks.cases.basic import BasicTestCase
    from benchmarks.cases.raw_pubsub import RawPubSubTestCase

    results_file = Path(__file__).resolve().parent / "results" / "benches.csv"
    cases_results: list[tuple[Any, MeasureResult]] = []

    cases: list[Any]
    if args.all:
        cases = [RawPubSubTestCase(), BasicTestCase()]
    elif args.case == "basic":
        cases = [BasicTestCase()]
    else:
        cases = [RawPubSubTestCase()]

    print(f"\nFastPubSub Benchmark Suite v{__version__}")
    print(f"Duration: {args.duration}s per case")
    print(f"Python: {platform.python_version()}")
    print("-" * 60)

    for case in cases:
        print(f"\nStarting benchmark: {case.case_name}")
        print(f"Description: {case.description}")
        print()

        result = await run_benchmark(case, args.duration)
        cases_results.append((case, result))

        # Save results immediately after each case
        save_results(case, result, results_file)

    # Print summary table
    print_results(cases_results)
    print(f"\nResults saved to: {results_file}")


if __name__ == "__main__":
    asyncio.run(main())
