"""Summary of benchmark results.

Examples:
    python -m benchmarks.report
    python -m benchmarks.report --run-id 3f2a9c1b7d4e
    python -m benchmarks.report --scenario cpu-and-wait-100ms
"""

import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

RESULTS_FILE = Path(__file__).resolve().parent / "results" / "runs.csv"
REFERENCE_CASE = "google-nonblocking"
METRICS = [
    ("EPS", "EPS"),
    ("CPU µs/event", "CPU Microseconds per Event"),
    ("p99 ms", "Latency p99 ms"),
]


def read_rows(
    results_file: Path,
    run_id: str | None = None,
    scenario: str | None = None,
) -> list[dict[str, str]]:
    """Read the result rows.

    Args:
        results_file: The CSV file.
        run_id: Keep only the rows of this run.
        scenario: Keep only the rows of this scenario.

    Returns:
        The rows.
    """
    if not results_file.exists():
        return []
    with results_file.open(newline="") as csv_file:
        rows = list(csv.DictReader(csv_file, delimiter=";"))
    return [
        row
        for row in rows
        if (run_id is None or row["Run Id"] == run_id)
        and (scenario is None or row["Scenario"] == scenario)
    ]


def format_metric(values: list[float]) -> str:
    """Format the median and the range of a metric.

    Args:
        values: The values of all repeats.

    Returns:
        The median, followed by the range when there is more than one value.
    """
    median = statistics.median(values)
    if len(values) == 1:
        return f"{median:,.1f}"
    return f"{median:,.1f} [{min(values):,.1f}-{max(values):,.1f}]"


def print_summary(rows: list[dict[str, str]]) -> None:
    """Print the median and the range for each label, scenario and case.

    The change column compares the median CPU per event with the
    ``google-nonblocking`` case of the same label and scenario.

    Args:
        rows: The result rows.
    """
    groups: dict[tuple[str, str], dict[str, list[dict[str, str]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for row in rows:
        groups[(row["Label"], row["Scenario"])][row["Case"]].append(row)

    for (label, scenario), cases in groups.items():
        print(f"\n{label} / {scenario}")
        header = f"{'Case':<20} | {'Runs':>4} | " + " | ".join(
            f"{title:>28}" for title, _ in METRICS
        )
        print(header + f" | {'CPU vs ' + REFERENCE_CASE:>28}")
        print("-" * (len(header) + 31))

        reference_rows = cases.get(REFERENCE_CASE, [])
        reference_cpu = (
            statistics.median(
                float(row["CPU Microseconds per Event"])
                for row in reference_rows
            )
            if reference_rows
            else 0.0
        )

        for case_name, case_rows in cases.items():
            cells = [
                format_metric([float(row[column]) for row in case_rows])
                for _, column in METRICS
            ]
            case_cpu = statistics.median(
                float(row["CPU Microseconds per Event"]) for row in case_rows
            )
            change = (
                f"{(case_cpu - reference_cpu) / reference_cpu * 100:+.1f}%"
                if reference_cpu
                else "-"
            )
            print(
                f"{case_name:<20} | {len(case_rows):>4} | "
                + " | ".join(f"{cell:>28}" for cell in cells)
                + f" | {change:>28}"
            )


def main() -> None:
    """Parse the arguments and print the summary."""
    parser = argparse.ArgumentParser(description="FastPubSub benchmark report")
    parser.add_argument("--results-file", type=Path, default=RESULTS_FILE)
    parser.add_argument("--run-id")
    parser.add_argument("--scenario")
    args = parser.parse_args()
    print_summary(read_rows(args.results_file, args.run_id, args.scenario))


if __name__ == "__main__":
    main()
