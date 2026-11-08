"""Benchmark orchestrator for FastPubSub.

The orchestrator runs each case in a new process with a new topic and a new
subscription. It repeats each case and interleaves the cases in each repeat,
so slow changes of the host affect all cases equally. It writes one CSV row
for each run and prints the median and the range for each case.

Examples:
    python -m benchmarks.bench --scenario echo --repeats 5
    python -m benchmarks.bench --scenario all --duration 30 --warmup 10
    python -m benchmarks.bench --case fastpubsub-async \\
        --fastpubsub-source .worktrees/main --label python-current
"""

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import sysconfig
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from benchmarks.cases import CASE_LOCATIONS
from benchmarks.report import print_summary, read_rows
from benchmarks.worker import RESULT_PREFIX
from benchmarks.workload import SCENARIOS, Scenario, calibrate_cpu_iterations

RESULTS_FILE = Path(__file__).resolve().parent / "results" / "runs.csv"
WORKER_TIMEOUT_MARGIN_SECONDS = 180

CSV_COLUMNS = [
    "Run Id",
    "Label",
    "Git Commit",
    "FastPubSub Version",
    "Scenario",
    "Case",
    "Transport",
    "Repeat",
    "Events",
    "Elapsed Seconds",
    "EPS",
    "CPU Seconds",
    "CPU Microseconds per Event",
    "Latency p50 ms",
    "Latency p95 ms",
    "Latency p99 ms",
    "Redeliveries",
    "Peak RSS MB",
    "In-flight Messages",
    "Payload Bytes",
    "CPU Work ms",
    "Wait ms",
    "CPU Iterations",
    "Python Version",
    "Host CPU",
    "Host CPU Limit",
    "Timestamp",
]


def read_git_commit(folder: Path) -> str:
    """Read the short commit of a git checkout.

    Args:
        folder: A folder inside the checkout.

    Returns:
        The short commit hash, with a ``-dirty`` suffix for changed files.
    """
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(folder), "rev-parse", "--short", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        changes = subprocess.check_output(
            ["git", "-C", str(folder), "status", "--porcelain"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"{commit}-dirty" if changes else commit


def read_python_version() -> str:
    """Read the Python version, with a ``t`` suffix for free threading.

    Returns:
        The Python version, for example ``3.14.1t``.
    """
    is_free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    return platform.python_version() + ("t" if is_free_threaded else "")


def read_host_cpu() -> str:
    """Read the CPU model of the host.

    Returns:
        The CPU model name.
    """
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor() or platform.machine()


def read_host_cpu_limit() -> str:
    """Read the CPU limit of the container, or the CPU count of the host.

    Returns:
        The number of CPUs that the process can use.
    """
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    if cpu_max.exists():
        quota, period = cpu_max.read_text().split()
        if quota != "max":
            return f"{int(quota) / int(period):g}"
    return str(os.cpu_count())


def run_worker(
    case_name: str,
    scenario: Scenario,
    args: argparse.Namespace,
    cpu_iterations: int,
) -> dict[str, Any] | None:
    """Run one case in a new process.

    Args:
        case_name: The benchmark case name.
        scenario: The scenario.
        args: The command line arguments.
        cpu_iterations: The loop rounds for 1 ms of CPU work.

    Returns:
        The worker result, or None when the worker fails.
    """
    command = [
        sys.executable,
        "-m",
        "benchmarks.worker",
        "--case",
        case_name,
        "--scenario",
        scenario.name,
        "--duration",
        str(args.duration),
        "--warmup",
        str(args.warmup),
        "--cpu-iterations",
        str(cpu_iterations),
        "--transport",
        args.transport,
    ]
    if args.fastpubsub_source:
        command += ["--fastpubsub-source", args.fastpubsub_source]

    try:
        completed = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            text=True,
            timeout=args.duration
            + args.warmup
            + WORKER_TIMEOUT_MARGIN_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        print(f"  {case_name}: the worker timed out")
        return None

    for line in completed.stdout.splitlines():
        if line.startswith(RESULT_PREFIX):
            result: dict[str, Any] = json.loads(line[len(RESULT_PREFIX) :])
            return result

    error_lines = completed.stderr.strip().splitlines()[-5:]
    print(f"  {case_name}: the worker failed")
    for error_line in error_lines:
        print(f"    {error_line}")
    return None


def build_row(
    result: dict[str, Any],
    case_name: str,
    scenario: Scenario,
    repeat: int,
    run_context: dict[str, str],
) -> dict[str, str]:
    """Build one CSV row.

    Args:
        result: The worker result.
        case_name: The benchmark case name.
        scenario: The scenario.
        repeat: The repeat number, from 1.
        run_context: The values that are the same for all rows of a run.

    Returns:
        The CSV row.
    """
    events = result["events"]
    elapsed_seconds = result["elapsed_seconds"]
    cpu_seconds = result["cpu_seconds"]
    events_per_second = events / elapsed_seconds if elapsed_seconds else 0.0
    cpu_per_event = cpu_seconds * 1_000_000 / events if events else 0.0
    return {
        "Run Id": run_context["run_id"],
        "Label": run_context["label"],
        "Git Commit": run_context["git_commit"],
        "FastPubSub Version": result["fastpubsub_version"],
        "Scenario": scenario.name,
        "Case": case_name,
        "Transport": run_context["transport"],
        "Repeat": str(repeat),
        "Events": str(events),
        "Elapsed Seconds": f"{elapsed_seconds:.2f}",
        "EPS": f"{events_per_second:.2f}",
        "CPU Seconds": f"{cpu_seconds:.2f}",
        "CPU Microseconds per Event": f"{cpu_per_event:.1f}",
        "Latency p50 ms": f"{result['latency_p50_ms']:.2f}",
        "Latency p95 ms": f"{result['latency_p95_ms']:.2f}",
        "Latency p99 ms": f"{result['latency_p99_ms']:.2f}",
        "Redeliveries": str(result["redeliveries"]),
        "Peak RSS MB": f"{result['peak_rss_mb']:.1f}",
        "In-flight Messages": str(scenario.in_flight_messages),
        "Payload Bytes": str(len(scenario.build_payload())),
        "CPU Work ms": f"{scenario.cpu_milliseconds:g}",
        "Wait ms": f"{scenario.wait_milliseconds:g}",
        "CPU Iterations": run_context["cpu_iterations"],
        "Python Version": run_context["python_version"],
        "Host CPU": run_context["host_cpu"],
        "Host CPU Limit": run_context["host_cpu_limit"],
        "Timestamp": datetime.now(tz=UTC).isoformat(),
    }


def append_row(results_file: Path, row: dict[str, str]) -> None:
    """Append one row to the results file.

    Args:
        results_file: The CSV file.
        row: The CSV row.
    """
    is_new_file = not results_file.exists()
    if not is_new_file:
        with results_file.open(newline="") as csv_file:
            header = csv_file.readline().strip().split(";")
        if header != CSV_COLUMNS:
            raise SystemExit(
                f"The columns of {results_file} do not match. "
                "Use --results-file with a new file."
            )
    with results_file.open("a", newline="") as csv_file:
        writer = csv.DictWriter(
            csv_file, fieldnames=CSV_COLUMNS, delimiter=";"
        )
        if is_new_file:
            writer.writeheader()
        writer.writerow(row)


def parse_arguments() -> argparse.Namespace:
    """Parse the command line arguments.

    Returns:
        The arguments.
    """
    parser = argparse.ArgumentParser(
        description="FastPubSub Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--scenario",
        nargs="+",
        choices=[*SCENARIOS, "all"],
        default=["echo"],
        help="Scenarios to run (default: echo)",
    )
    parser.add_argument(
        "--case",
        nargs="+",
        choices=[*CASE_LOCATIONS, "all"],
        default=["all"],
        help="Cases to run (default: all)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30,
        help="Measured seconds for each run (default: 30)",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=10,
        help="Seconds before the measurement starts (default: 10)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=5,
        help="Runs for each case and scenario (default: 5)",
    )
    parser.add_argument(
        "--label",
        default="local",
        help="A name for the code under test, for example python-optimized",
    )
    parser.add_argument(
        "--transport",
        choices=["python", "rust"],
        default="python",
        help="The FastPubSub transport (default: python)",
    )
    parser.add_argument(
        "--fastpubsub-source",
        help="A folder that contains a different fastpubsub package",
    )
    parser.add_argument(
        "--git-commit",
        help="The commit of the code under test (default: read with git)",
    )
    parser.add_argument(
        "--results-file",
        type=Path,
        default=RESULTS_FILE,
        help=f"The CSV file for the results (default: {RESULTS_FILE})",
    )
    return parser.parse_args()


def main() -> None:
    """Run the selected cases and scenarios."""
    args = parse_arguments()
    scenario_names = (
        list(SCENARIOS) if "all" in args.scenario else args.scenario
    )
    case_names = list(CASE_LOCATIONS) if "all" in args.case else args.case

    code_folder = Path(args.fastpubsub_source or Path.cwd())
    print("Calibrating 1 ms of CPU work...")
    cpu_iterations = calibrate_cpu_iterations()
    run_context = {
        "run_id": uuid.uuid4().hex[:12],
        "label": args.label,
        "git_commit": args.git_commit or read_git_commit(code_folder),
        "cpu_iterations": str(cpu_iterations),
        "transport": args.transport,
        "python_version": read_python_version(),
        "host_cpu": read_host_cpu(),
        "host_cpu_limit": read_host_cpu_limit(),
    }

    print(f"Run id: {run_context['run_id']}")
    print(f"Label: {args.label} (commit {run_context['git_commit']})")
    print(f"Python: {run_context['python_version']}")
    print(f"Transport: {args.transport}")
    print(
        f"Host CPU: {run_context['host_cpu']} "
        f"(limit {run_context['host_cpu_limit']})"
    )
    print(f"CPU iterations for 1 ms: {cpu_iterations}")
    print(
        f"Duration: {args.duration}s, warm-up: {args.warmup}s, "
        f"repeats: {args.repeats}"
    )

    args.results_file.parent.mkdir(parents=True, exist_ok=True)
    for scenario_name in scenario_names:
        scenario = SCENARIOS[scenario_name]
        print(f"\nScenario {scenario.name}: {scenario.description}")
        for repeat in range(1, args.repeats + 1):
            print(f" Repeat {repeat}/{args.repeats}")
            for case_name in case_names:
                result = run_worker(case_name, scenario, args, cpu_iterations)
                if result is None:
                    continue
                row = build_row(
                    result, case_name, scenario, repeat, run_context
                )
                append_row(args.results_file, row)
                print(
                    f"  {case_name}: EPS {row['EPS']}, "
                    f"CPU {row['CPU Microseconds per Event']} µs/event, "
                    f"p99 {row['Latency p99 ms']} ms, "
                    f"redeliveries {row['Redeliveries']}"
                )

    print_summary(read_rows(args.results_file, run_id=run_context["run_id"]))
    print(f"\nResults saved to: {args.results_file}")


if __name__ == "__main__":
    main()
