"""FastPubSub command-line interface."""

import rich
import typer

from fastpubsub.__about__ import __version__
from fastpubsub.cli.options import (
    AppArgument,
    AppHostOption,
    AppHotReloadOption,
    AppLogColorizeOption,
    AppLogLevelOption,
    AppLogSerializeOption,
    AppNumWorkersOption,
    AppPortOption,
    AppSelectedSubscribersOption,
    AppServerLogLevelOption,
    AppVersionOption,
    CLIContext,
)
from fastpubsub.cli.runner import AppConfiguration, ApplicationRunner, ServerConfiguration
from fastpubsub.cli.utils import LogLevels, ensure_pubsub_credentials

app = typer.Typer(
    name="fastpubsub",
    help="A CLI to run FastPubSub applications and interact with Pub/Sub (locally and on cloud).",
    pretty_exceptions_short=True,
    invoke_without_command=True,
    rich_markup_mode="markdown",
)

# V2: this command and its subcommands will be released on the future"
"""
pubsub = typer.Typer(
    name="pubsub",
    help="Commands for interacting with Google Cloud Pub/Sub.",
    rich_markup_mode="markdown",
)

pubsub_cloud = typer.Typer(
    name="cloud",
    help="Subcommand to interact with Cloud-based Pub/Sub.",
    rich_markup_mode="markdown",
)

pubsub_local = typer.Typer(
    name="local",
    help="Subcommand to interact with Pub/Sub locally (e.g., emulator).",
    rich_markup_mode="markdown",
)

pubsub.add_typer(pubsub_cloud)
pubsub.add_typer(pubsub_local)
app.add_typer(pubsub)
"""


@app.callback()
def main(
    ctx: CLIContext,
    version: AppVersionOption = False,
) -> None:
    """Display helpful tips when the main command is run without any subcommands."""
    if ctx.invoked_subcommand is None and not version:
        rich.print("\n[bold]Welcome to the FastPubSub CLI! ✨[/bold]")
        rich.print("\n[dim]A CLI to run FastPubSub applications and interact with Pub/Sub.[/dim]")
        rich.print("\n[bold]Usage[/bold]: [cyan]fastpubsub [COMMAND] [ARGS]...[/cyan]")
        rich.print("\n[bold]Common Commands:[/bold]")
        rich.print("  [green]run[/green]    Run a FastPubSub application.")
        rich.print("  [green]help[/green]   Get detailed help for a command.")
        rich.print(
            "\nRun '[cyan]fastpubsub --help[/cyan]' for "
            "a list of all available commands and options."
        )
        rich.print(
            "For more information, visit our documentation at "
            "[link=https://github.com/matheusvnm/fastpubsub]https://github.com/matheusvnm/fastpubsub[/link]"
        )

    if version:
        import platform

        typer.echo(
            f"Running FastPubSub {__version__} with {platform.python_implementation()} "
            f"{platform.python_version()} on {platform.system()}",
        )

        raise typer.Exit


@app.command()
def run(
    app: AppArgument,
    workers: AppNumWorkersOption = 1,
    subscribers: AppSelectedSubscribersOption = [],
    reload: AppHotReloadOption = False,
    host: AppHostOption = "127.0.0.1",
    port: AppPortOption = 8000,
    log_level: AppLogLevelOption = LogLevels.INFO,
    log_serialize: AppLogSerializeOption = False,
    log_colorize: AppLogColorizeOption = False,
    server_log_level: AppServerLogLevelOption = LogLevels.WARNING,
) -> None:
    """Runs a FastPubSub application.

    Args:
        app: The application to run.
        workers: The number of worker processes.
        subscribers: The subscribers to run.
        reload: Whether to enable auto-reloading.
        host: The host to bind to.
        port: The port to bind to.
        log_level: The log level.
        log_serialize: Whether to serialize logs.
        log_colorize: Whether to colorize logs.
        server_log_level: The server (uvicorn) log level.
    """
    ensure_pubsub_credentials()
    app_configuration = AppConfiguration(
        app=app,
        log_level=log_level.upper(),
        log_serialize=log_serialize,
        log_colorize=log_colorize,
        subscribers=set(subscribers) if subscribers else set(),
    )

    server_configuration = ServerConfiguration(
        host=host,
        port=port,
        workers=workers,
        reload=reload,
        log_level=server_log_level.lower(),
    )

    application_runner = ApplicationRunner(app_configuration, server_configuration)
    application_runner.setup()
    application_runner.validate()
    application_runner.run()


@app.command(name="help")
def show_help(ctx: typer.Context) -> None:
    """Show this message and exit."""
    if ctx.parent:
        rich.print(ctx.parent.get_help())


def execute_app() -> None:
    """Execute the FastPubSub CLI application."""
    app()


if __name__ == "__main__":
    execute_app()
