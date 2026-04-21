"""Inspect command for FastPubSub CLI."""

import json

import rich
import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from fastpubsub.__about__ import __version__
from fastpubsub._internal import SubscriberSelector
from fastpubsub.cli.options import (
    AppArgument,
    InspectColumnsOption,
    InspectFilterOption,
    InspectFormatOption,
)
from fastpubsub.cli.utils import import_app
from fastpubsub.pubsub.subscriber import Subscriber

inspect_app = typer.Typer(
    name="inspect",
    help="Inspect a FastPubSub application's components.",
    rich_markup_mode="markdown",
)

DEFAULT_COLUMNS = ["alias", "project", "topic", "subscription", "handler"]


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


def _format_table(
    app_path: str,
    records: list[SubscriberRecord],
    columns: list[str],
) -> None:
    """Print subscribers as a Rich table.

    Args:
        app_path: The module:attribute path for the header.
        records: List of SubscriberRecord to display.
        columns: Column names to include.
    """
    count = len(records)
    header = (
        f"[bold]FastPubSub[/bold] (v{__version__}): "
        f"{app_path} — {count} subscriber{'s' if count != 1 else ''}"
    )
    rich.print(f"\n{header}\n")

    table = Table()
    for col in columns:
        table.add_column(col.replace("_", " ").title())

    for record in records:
        row = record.model_dump(include=set(columns))
        table.add_row(*[str(row[col]) for col in columns])

    console = Console()
    console.print(table)


def _format_json(
    app_path: str,
    records: list[SubscriberRecord],
    columns: list[str],
) -> None:
    """Print subscribers as JSON.

    Args:
        app_path: The module:attribute path.
        records: List of SubscriberRecord to display.
        columns: Column names to include.
    """
    output = {
        "app": app_path,
        "version": __version__,
        "subscribers": [
            record.model_dump(include=set(columns)) for record in records
        ],
    }
    typer.echo(json.dumps(output, indent=2, default=str))


@inspect_app.command(name="subscribers")
def inspect_subscribers(
    app: AppArgument,
    filter_patterns: InspectFilterOption = [],
    columns: InspectColumnsOption = None,
    output_format: InspectFormatOption = "table",
) -> None:
    """List all subscribers in a FastPubSub application.

    Args:
        app: The application path in module:attribute format.
        filter_patterns: Glob patterns to filter subscribers by alias.
        columns: Comma-separated column names, or 'all'.
        output_format: Output format: 'table' or 'json'.
    """
    resolved_columns = resolve_columns(columns)

    fastpubsub_app = import_app(app)
    subscribers = fastpubsub_app.broker.router.get_subscribers()

    if filter_patterns:
        selector = SubscriberSelector(patterns=set(filter_patterns))
        filtered_list = selector.select(subscribers)
        subscribers = {
            alias: sub
            for alias, sub in subscribers.items()
            if sub in filtered_list
        }

    records = sorted(
        [
            SubscriberRecord.from_subscriber(alias, sub)
            for alias, sub in subscribers.items()
        ],
        key=lambda r: r.alias,
    )

    if output_format == "json":
        _format_json(app, records, resolved_columns)
    else:
        _format_table(app, records, resolved_columns)


# Future inspect subcommands:
# - inspect topics: list unique topics and their subscribers (fan-out view)
# - inspect publishers: list registered publishers and target topics
# - inspect routes: show router hierarchy, prefixes, and nesting
# - inspect middlewares: show middleware chains per subscriber and global
# - inspect policies: summary table of retry, DLT, and delivery policies
# - inspect config: resolved runtime config (project, host, port, env vars)
