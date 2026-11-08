"""Benchmark cases.

The registry imports each case module only when a worker needs it. A worker
can then select the FastPubSub source code before the first import.
"""

import importlib
from contextlib import AbstractAsyncContextManager
from typing import Protocol

from benchmarks.resources import RunResources
from benchmarks.workload import Recorder, Scenario

CASE_LOCATIONS = {
    "google-blocking": ("benchmarks.cases.google", "GoogleBlockingCase"),
    "google-nonblocking": ("benchmarks.cases.google", "GoogleNonBlockingCase"),
    "fastpubsub-async": ("benchmarks.cases.fastpubsub", "FastPubSubAsyncCase"),
    "fastpubsub-sync": ("benchmarks.cases.fastpubsub", "FastPubSubSyncCase"),
    "fastpubsub-rust": ("benchmarks.cases.rust", "FastPubSubRustCase"),
}


class BenchmarkCase(Protocol):
    """The interface of all benchmark cases."""

    name: str
    description: str

    def run(self) -> AbstractAsyncContextManager[None]:
        """Start the echo loop and stop it at exit."""
        ...


class BenchmarkCaseFactory(Protocol):
    """The constructor of a benchmark case."""

    def __call__(
        self,
        scenario: Scenario,
        cpu_iterations: int,
        resources: RunResources,
        recorder: Recorder,
    ) -> BenchmarkCase:
        """Create the benchmark case."""
        ...


def load_case(case_name: str) -> BenchmarkCaseFactory:
    """Import a benchmark case class.

    Args:
        case_name: The benchmark case name.

    Returns:
        The benchmark case class.
    """
    module_name, class_name = CASE_LOCATIONS[case_name]
    module = importlib.import_module(module_name)
    case_class: BenchmarkCaseFactory = getattr(module, class_name)
    return case_class
