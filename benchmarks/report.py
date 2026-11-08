"""Summary of benchmark results.

Examples:
    python -m benchmarks.report
    python -m benchmarks.report --run-id 3f2a9c1b7d4e
    python -m benchmarks.report --scenario cpu-and-wait-100ms
    python -m benchmarks.report --by-scenario
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


def build_row_name(row: dict[str, str]) -> str:
    """Name a row by its case, transport and Python version."""
    transport = row.get("Transport") or "python"
    return f"{row['Case']} {transport} {row['Python Version']}"


def print_summary(
    rows: list[dict[str, str]], is_grouped_by_scenario: bool = False
) -> None:
    """Print the median and the range for each label, scenario and case.

    The change column compares the median CPU per event with the
    ``google-nonblocking`` case of the same group. Without that case, it
    compares with the first case of the group.

    Args:
        rows: The result rows.
        is_grouped_by_scenario: Put all labels of a scenario in one table.
            Each row name then has the case, the transport and the Python
            version.
    """
    groups: dict[tuple[str, str], dict[str, list[dict[str, str]]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    for row in rows:
        if is_grouped_by_scenario:
            key = ("all labels", row["Scenario"])
            name = build_row_name(row)
        else:
            key = (row["Label"], row["Scenario"])
            name = row["Case"]
        groups[key][name].append(row)

    for (label, scenario), cases in groups.items():
        print(f"\n{label} / {scenario}")
        width = max(20, *(len(name) for name in cases))
        header = f"{'Case':<{width}} | {'Runs':>4} | " + " | ".join(
            f"{title:>28}" for title, _ in METRICS
        )
        print(header + f" | {'CPU change':>28}")
        print("-" * (len(header) + 31))

        reference_rows = next(
            (
                case_rows
                for name, case_rows in cases.items()
                if name.split()[0] == REFERENCE_CASE
            ),
            next(iter(cases.values())),
        )
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
                f"{case_name:<{width}} | {len(case_rows):>4} | "
                + " | ".join(f"{cell:>28}" for cell in cells)
                + f" | {change:>28}"
            )


def main() -> None:
    """Parse the arguments and print the summary."""
    parser = argparse.ArgumentParser(description="FastPubSub benchmark report")
    parser.add_argument("--results-file", type=Path, default=RESULTS_FILE)
    parser.add_argument("--run-id")
    parser.add_argument("--scenario")
    parser.add_argument("--by-scenario", action="store_true")
    args = parser.parse_args()
    print_summary(
        read_rows(args.results_file, args.run_id, args.scenario),
        is_grouped_by_scenario=args.by_scenario,
    )


if __name__ == "__main__":
    main()
