"""Inspect command for FastPubSub CLI."""

import json

import rich
import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from fastpubsub.__about__ import __version__
from fastpubsub._internal import SubscriberSelector
from fastpubsub.applications import FastPubSub
from fastpubsub.pubsub.subscriber import Subscriber

inspect_app = typer.Typer(
    name="inspect",
    help="Inspect a FastPubSub application's components.",
    rich_markup_mode="markdown",
)

DEFAULT_COLUMNS = [
    "alias",
    "project",
    "topic",
    "subscription",
    "handler",
]


class SubscriberRecord(BaseModel):
    """Flat representation of a subscriber for inspection output."""

    # Core fields (shown by default)
    alias: str
    project: str
    topic: str
    subscription: str
    handler: str

    # Policy fields (available via --columns)
    ack_deadline: int = 0
    filter: str = ""
    ordering: bool = False
    exactly_once: bool = False
    max_messages: int = 0
    retry_min: int = 0
    retry_max: int = 0
    dead_letter_topic: str = ""
    dead_letter_max_attempts: int = 0
    autocreate: bool = True
    autoupdate: bool = False

    @classmethod
    def from_subscriber(
        cls, alias: str, subscriber: Subscriber
    ) -> "SubscriberRecord":
        """Create a SubscriberRecord from an alias and Subscriber.

        Args:
            alias: The resolved subscriber alias.
            subscriber: The Subscriber instance.

        Returns:
            A SubscriberRecord with all fields populated.
        """
        return cls(
            alias=alias,
            project=subscriber.project_id,
            topic=subscriber.topic_name,
            subscription=subscriber.subscription_name,
            handler=getattr(subscriber.func, "__name__", ""),
            ack_deadline=subscriber.delivery_policy.ack_deadline_seconds,
            filter=subscriber.delivery_policy.filter_expression,
            ordering=subscriber.delivery_policy.enable_message_ordering,
            exactly_once=(
                subscriber.delivery_policy.enable_exactly_once_delivery
            ),
            max_messages=subscriber.control_flow_policy.max_messages,
            retry_min=subscriber.retry_policy.min_backoff_delay_secs,
            retry_max=subscriber.retry_policy.max_backoff_delay_secs,
            dead_letter_topic=(
                subscriber.dead_letter_policy.topic_name
                if subscriber.dead_letter_policy
                else ""
            ),
            dead_letter_max_attempts=(
                subscriber.dead_letter_policy.max_delivery_attempts
                if subscriber.dead_letter_policy
                else 0
            ),
            autocreate=subscriber.lifecycle_policy.autocreate,
            autoupdate=subscriber.lifecycle_policy.autoupdate,
        )


ALL_COLUMNS = list(SubscriberRecord.model_fields.keys())


def resolve_columns(columns_arg: str | None) -> list[str]:
    """Resolve the --columns argument into a list of column names.

    Args:
        columns_arg: Raw value from the --columns CLI option, or None
            for defaults.

    Returns:
        A list of validated column names.

    Raises:
        typer.BadParameter: If any column name is unknown.
    """
    if columns_arg is None:
        return list(DEFAULT_COLUMNS)

    tokens = [t.strip().lower() for t in columns_arg.split(",")]
    tokens = [t for t in tokens if t]

    if tokens == ["all"]:
        return list(ALL_COLUMNS)

    unknown = [t for t in tokens if t not in SubscriberRecord.model_fields]
    if unknown:
        available = ", ".join(ALL_COLUMNS)
        typer.echo(
            f"Error: Unknown column(s): {', '.join(unknown)}. "
            f"Available: {available}",
            err=True,
        )
        raise SystemExit(1)

    return tokens


class SubscriberInspector:
    """Inspects subscribers of a FastPubSub application."""

    def __init__(
        self,
        app_instance: FastPubSub,
        app_path: str,
        columns: list[str],
        filter_patterns: set[str],
    ) -> None:
        """Initialize the SubscriberInspector.

        Args:
            app_instance: The FastPubSub application instance.
            app_path: The module:attribute path string.
            columns: Resolved column names to display.
            filter_patterns: Glob patterns for alias filtering.
        """
        self.app_instance = app_instance
        self.app_path = app_path
        self.columns = columns
        self.filter_patterns = filter_patterns

    def inspect(self, output_format: str) -> None:
        """Run the inspection and print output.

        Args:
            output_format: The output format ('table' or 'json').
        """
        records = self._build_records()

        if output_format == "json":
            return self._print_json(records)

        return self._print_table(records)

    def _build_records(self) -> list[SubscriberRecord]:
        """Build sorted SubscriberRecord list from the app.

        Returns:
            A sorted list of SubscriberRecord instances.
        """
        subscribers = self.app_instance.broker.router.get_subscribers()

        if self.filter_patterns:
            selector = SubscriberSelector(patterns=self.filter_patterns)
            filtered_list = selector.select(subscribers)
            subscribers = {
                alias: sub
                for alias, sub in subscribers.items()
                if sub in filtered_list
            }

        return sorted(
            [
                SubscriberRecord.from_subscriber(alias, sub)
                for alias, sub in subscribers.items()
            ],
            key=lambda r: r.alias,
        )

    def _print_table(self, records: list[SubscriberRecord]) -> None:
        """Print subscribers as a Rich table.

        Args:
            records: List of SubscriberRecord to display.
        """
        count = len(records)
        header = (
            f"[bold]FastPubSub[/bold] (v{__version__}): "
            f"{self.app_path} — "
            f"{count} subscriber{'s' if count != 1 else ''}"
        )
        rich.print(f"\n{header}\n")

        table = Table()
        for col in self.columns:
            table.add_column(col.replace("_", " ").title())

        for record in records:
            row = record.model_dump(include=set(self.columns))
            table.add_row(*[str(row[col]) for col in self.columns])

        console = Console()
        console.print(table)

    def _print_json(self, records: list[SubscriberRecord]) -> None:
        """Print subscribers as JSON.

        Args:
            records: List of SubscriberRecord to display.
        """
        output = {
            "app": self.app_path,
            "version": __version__,
            "subscribers": [
                record.model_dump(include=set(self.columns))
                for record in records
            ],
        }
        typer.echo(json.dumps(output, indent=2, default=str))


# Future inspect subcommands:
# - inspect topics: list unique topics and their subscribers (fan-out view)
# - inspect publishers: list registered publishers and target topics
# - inspect routes: show router hierarchy, prefixes, and nesting
# - inspect middlewares: show middleware chains per subscriber and global
# - inspect policies: summary table of retry, DLT, and delivery policies
# - inspect config: resolved runtime config (project, host, port, env vars)
