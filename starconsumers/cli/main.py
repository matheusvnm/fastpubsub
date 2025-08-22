from pathlib import Path
from typing import Annotated

import rich
import typer

from starconsumers.cli.discover import ApplicationDiscover
from starconsumers.cli.runner import ApplicationRunner, ServerConfiguration

cli_app = typer.Typer(
    help="A CLI to discover and run StarConsumers applications with Uvicorn.",
    invoke_without_command=True,
    rich_markup_mode="markdown",
)


@cli_app.callback()
def main(ctx: typer.Context) -> None:
    """
    Display helpful tips when the main command is run without any subcommands.
    """
    if ctx.invoked_subcommand is None:
        rich.print("\n[bold]Welcome to the StarConsumers CLI! ✨[/bold]")
        rich.print("\nUsage Tips:")
        rich.print("  - To start your application, use the `run` command: `starconsumers run`")
        rich.print("  - To see all options for running the app: `starconsumers run --help`")
        rich.print("  - For a detailed guide with examples: `starconsumers help`")


@cli_app.command()
def run(
    path: Annotated[
        Path | None,
        typer.Argument(help="Path to a Python file containing the StarConsumers application."),
    ] = None,
    *,
    tasks: Annotated[
        list[str] | None,
        typer.Option(
            "--tasks",
            help="Specify a tasks to run. Use this option multiple times for multiple tasks.",
        ),
    ] = [],
    host: Annotated[
        str,
        typer.Option(help="The host to serve the application on. Use '0.0.0.0' for public access."),
    ] = "0.0.0.0",
    port: Annotated[
        int,
        typer.Option(help="The port to serve the application on."),
    ] = 8000,
    reload: Annotated[
        bool,
        typer.Option(help="Enable auto-reload when code files change. Ideal for development."),
    ] = False,
    root_path: Annotated[
        str,
        typer.Option(
            help="Root path prefix for the application, useful when behind a reverse proxy."
        ),
    ] = "",
    app_name: Annotated[
        str | None,
        typer.Option(
            help="Name of the variable containing the application. "
            "If not provided, it will be auto-detected."
        ),
    ] = None,
) -> None:
    """
    Run a StarConsumers application using Uvicorn.

    This tool simplifies running applications by automatically discovering the
    module and application object. If no path is given, it will try common
    paths like 'main.py' or 'app/main.py'.

    You can also specify which consumers tasks to run using the --tasks option.
    """
    application_discover = ApplicationDiscover()
    application_location = application_discover.search_application(path=path, app_name=app_name)

    server_configuration = ServerConfiguration(
        host=host,
        port=port,
        reload=reload,
        root_path=root_path,
        tasks=tasks,
    )

    application_runner = ApplicationRunner()
    application_runner.run(application_location, server_configuration)


@cli_app.command(name="help")
def show_help() -> None:
    """
    Shows a helpful guide with common use cases and examples.
    """
    explanation = """
    [bold cyan]StarConsumers CLI Usage Guide[/bold cyan]

    This tool is designed to get your application running with minimal effort.
    Here are some common scenarios and how to handle them:

    [bold]1. Basic Usage (Auto-Discovery)[/bold]
    If your project has a standard structure (e.g., app/main.py with a variable named 'app'),
    you can simply run:
    [yellow]> starconsumers run[/yellow]

    [bold]2. Specifying a Path[/bold]
    If your main file is in a different location:
    [yellow]> starconsumers run my_project/server.py[/yellow]

    [bold]3. Running Specific Tasks[/bold]
    To run only `task_a` and `task_b`:
    [yellow]> starconsumers run --tasks task_a --tasks task_b[/yellow]

    [bold]4. Running in Development Mode[/bold]
    To enable auto-reload on code changes, use the `--reload` flag:
    [yellow]> starconsumers run --reload[/yellow]

    [bold]5. Combining Options[/bold]
    You can combine these options as needed. For example, to run a specific
    consumer from a specific file in development mode:
    [yellow]> starconsumers run my_app.py --tasks email_sender --reload[/yellow]
    """
    rich.print(explanation)


if __name__ == "__main__":
    cli_app()
