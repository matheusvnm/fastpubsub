"""Record the memory and CPU timeline of one benchmark worker.

The sampler starts the worker, reads its resident memory and the CPU
time of each thread at a fixed interval, and writes one CSV row for each
sample. The worker prints phase markers, so each row has a phase name.
"""

import argparse
import csv
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import psutil

from benchmarks.worker import PHASE_PREFIX, RESULT_PREFIX

THREAD_GROUPS = {
    "fastpubsub-core": "tokio",
    "fastpubsub-netw": "tokio",
    "fastpubsub-brid": "bridge",
    "asyncio_": "executor",
    "ThreadPoolExecu": "executor",
    "grpc_global_tim": "grpc",
    "Thread-": "google",
    "python": "main",
}


def read_thread_name(process_id: int, thread_id: int) -> str:
    """Read the name of one thread on Linux.

    Args:
        process_id: The process id.
        thread_id: The thread id.

    Returns:
        The thread name, or ``thread`` when the name is not available.
    """
    path = Path(f"/proc/{process_id}/task/{thread_id}/comm")
    try:
        return path.read_text().strip()
    except OSError:
        return "thread"


def find_thread_group(process_id: int, thread_id: int) -> str:
    """Find the group of one thread from its name.

    Args:
        process_id: The process id.
        thread_id: The thread id.

    Returns:
        The group name.
    """
    if thread_id == process_id:
        return "main"
    name = read_thread_name(process_id, thread_id)
    for prefix, group in THREAD_GROUPS.items():
        if name.startswith(prefix):
            return group
    return "other"


def read_thread_times(process: psutil.Process) -> list[tuple[int, float]]:
    """Read the CPU time of each thread of a process.

    macOS does not give the threads of another process without root
    access. There, the whole process counts as the main thread.

    Args:
        process: The process to read.

    Returns:
        The thread id and its CPU time in seconds, for each thread.
    """
    try:
        return [
            (thread.id, thread.user_time + thread.system_time)
            for thread in process.threads()
        ]
    except psutil.AccessDenied:
        cpu_times = process.cpu_times()
        return [(process.pid, cpu_times.user + cpu_times.system)]


class PhaseReader:
    """Read the phase markers and the result line of the worker."""

    def __init__(self, worker: subprocess.Popen[str]) -> None:
        """Start the reader thread.

        Args:
            worker: The worker process.
        """
        self.phase = "setup"
        self.result_line = ""
        self._worker = worker
        self._thread = threading.Thread(target=self._read, daemon=True)
        self._thread.start()

    def _read(self) -> None:
        assert self._worker.stdout is not None
        for line in self._worker.stdout:
            if line.startswith(PHASE_PREFIX):
                self.phase = line.removeprefix(PHASE_PREFIX).strip()
            elif line.startswith(RESULT_PREFIX):
                self.result_line = line.strip()

    def join(self) -> None:
        """Wait until the worker closes its output."""
        self._thread.join(timeout=10)


def sample_worker(
    command: list[str], interval: float, output: Path, label: str
) -> None:
    """Run the worker and write its timeline.

    Args:
        command: The worker command.
        interval: The time between two samples, in seconds.
        output: The CSV file to append the rows to.
        label: The run label for each row.
    """
    worker = subprocess.Popen(  # noqa: S603
        command, stdout=subprocess.PIPE, text=True
    )
    reader = PhaseReader(worker)
    process = psutil.Process(worker.pid)
    start = time.perf_counter()
    previous_thread_seconds: dict[int, float] = {}
    previous_time = start
    groups = sorted(set(THREAD_GROUPS.values()) | {"other"})
    is_new_file = not output.exists()

    with output.open("a", newline="") as file:
        writer = csv.writer(file)
        if is_new_file:
            writer.writerow(
                ["label", "seconds", "phase", "rss_mb", "cpu_percent"]
                + [f"cpu_{group}" for group in groups]
            )
        while worker.poll() is None:
            time.sleep(interval)
            try:
                rss_mb = process.memory_info().rss / (1024 * 1024)
                threads = read_thread_times(process)
            except psutil.Error:
                break
            now = time.perf_counter()
            elapsed = now - previous_time
            previous_time = now
            group_percent: dict[str, float] = defaultdict(float)
            for thread_id, seconds in threads:
                delta = seconds - previous_thread_seconds.get(thread_id, 0.0)
                previous_thread_seconds[thread_id] = seconds
                group = find_thread_group(worker.pid, thread_id)
                group_percent[group] += 100 * delta / elapsed
            writer.writerow(
                [
                    label,
                    f"{now - start:.2f}",
                    reader.phase,
                    f"{rss_mb:.1f}",
                    f"{sum(group_percent.values()):.1f}",
                ]
                + [f"{group_percent[group]:.1f}" for group in groups]
            )
    reader.join()
    print(f"{label}: {reader.result_line or 'no result'}")


def main() -> None:
    """Parse the arguments and record one timeline."""
    parser = argparse.ArgumentParser(description="Record a worker timeline")
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("worker_arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    worker_arguments = [
        argument for argument in args.worker_arguments if argument != "--"
    ]
    command = [sys.executable, "-m", "benchmarks.worker", *worker_arguments]
    sample_worker(command, args.interval, args.output, args.label)


if __name__ == "__main__":
    main()
