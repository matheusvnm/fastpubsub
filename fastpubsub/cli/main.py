import rich
import typer

from fastpubsub.__about__ import __version__
from fastpubsub.cli.options import (
    AppApmProvider,
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
from fastpubsub.cli.utils import LogLevels, ensure_pubsub_credentials, get_log_level

app = typer.Typer(
    help="A CLI to run FastPubSub applications and interact with Pub/Sub (locally and on cloud).",
    pretty_exceptions_short=True,
    invoke_without_command=True,
    rich_markup_mode="markdown",
)

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


@app.callback()
def main(
    ctx: CLIContext,
    version: AppVersionOption = False,
) -> None:
    """
    Display helpful tips when the main command is run without any subcommands.
    """
    if ctx.invoked_subcommand is None:
        # TODO: Add a better explanation
        rich.print("\n[bold]Welcome to the FastPubSub CLI! ✨[/bold]")
        rich.print("\nUsage Tips:")
        rich.print("  - To start your application, use the `run` command: `fastpubsub run`")
        rich.print(
            "  - To interact with Pub/Sub, use the `pubsub` command: `fastpubsub pubsub --help`"
        )
        rich.print("  - To see all options for a command: `fastpubsub <command> --help`")
        rich.print("  - For a detailed guide with examples: `fastpubsub help`")

    if version:
        import platform

        typer.echo(
            f"Running FastStream {__version__} with {platform.python_implementation()} "
            f"{platform.python_version()} on {platform.system()}",
        )

        raise typer.Exit


@app.command()
def run(
    app: AppArgument,
    workers: AppNumWorkersOption = 1,
    subscribers: AppSelectedSubscribersOption = [],
    reload: AppHotReloadOption = False,
    host: AppHostOption = "0.0.0.0",
    port: AppPortOption = 8000,
    log_level: AppLogLevelOption = LogLevels.info,
    log_serialize: AppLogSerializeOption = False,
    log_colorize: AppLogColorizeOption = False,
    server_log_level: AppServerLogLevelOption = LogLevels.warning,
    apm_provider: AppApmProvider = AppApmProvider.NOOP,
) -> None:
    ensure_pubsub_credentials()
    translated_log_level = get_log_level(log_level)
    app_configuration = AppConfiguration(
        app=app,
        log_level=translated_log_level,
        log_serialize=log_serialize,
        log_colorize=log_colorize,
        apm_provider=apm_provider,
        subscribers=set(subscribers) if subscribers else set(),
    )

    translated_server_log_level = get_log_level(server_log_level)
    server_configuration = ServerConfiguration(
        host=host,
        port=port,
        workers=workers,
        reload=reload,
        log_level=translated_server_log_level,
    )

    application_runner = ApplicationRunner()
    application_runner.run(app_configuration, server_configuration)


@app.command(name="help")
def show_help() -> None:
    pass


def execute_app() -> None:
    app()


if __name__ == "__main__":
    execute_app()
