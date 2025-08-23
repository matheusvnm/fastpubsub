import json
import os
from pathlib import Path
from typing import Annotated

import rich
import typer

from starconsumers.cli.discover import ApplicationDiscover
from starconsumers.cli.runner import ApplicationRunner, ServerConfiguration
from starconsumers.pubsub.publisher import PubSubPublisher

cli_app = typer.Typer(
    help="A CLI to discover and run StarConsumers applications and interact with Pub/Sub.",
    invoke_without_command=True,
    rich_markup_mode="markdown",
)

# Create a new Typer app for the 'pubsub' command group
pubsub_app = typer.Typer(
    name="pubsub",
    help="Commands for interacting with Google Cloud Pub/Sub.",
    rich_markup_mode="markdown",
)
cli_app.add_typer(pubsub_app)


@cli_app.callback()
def main(ctx: typer.Context) -> None:
    """
    Display helpful tips when the main command is run without any subcommands.
    """
    if ctx.invoked_subcommand is None:
        rich.print("\n[bold]Welcome to the StarConsumers CLI! ✨[/bold]")
        rich.print("\nUsage Tips:")
        rich.print("  - To start your application, use the `run` command: `starconsumers run`")
        rich.print(
            "  - To interact with Pub/Sub, use the `pubsub` command: `starconsumers pubsub --help`"
        )
        rich.print("  - To see all options for a command: `starconsumers <command> --help`")
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


@pubsub_app.command(name="create-topic")
def create_topic(
    topic_name: Annotated[
        str,
        typer.Argument(help="The name of the topic to create."),
    ],
    *,
    project_id: Annotated[
        str,
        typer.Option(help="The Google Cloud Project ID.", rich_help_panel="GCP Configuration"),
    ],
    emulator: Annotated[
        bool,
        typer.Option(help="Use the Pub/Sub emulator.", rich_help_panel="Emulator Configuration"),
    ] = False,
    emulator_port: Annotated[
        int | None,
        typer.Option(
            help="Port of the Pub/Sub emulator. Required if --emulator is used.",
            rich_help_panel="Emulator Configuration",
        ),
    ] = None,
) -> None:
    """
    Creates a new Pub/Sub topic in the specified project.
    """
    # Validation for emulator configuration
    if emulator and emulator_port is None:
        raise typer.BadParameter("The --emulator-port is required when --emulator is set.")

    emulator_env = os.getenv("PUBSUB_EMULATOR_HOST", "")
    if emulator and not emulator_env:
        raise typer.BadParameter(
            "The PUBSUB_EMULATOR_HOST env var is required when --emulator is set."
        )

    application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not emulator and not application_credentials:
        raise typer.BadParameter(
            "The GOOGLE_APPLICATION_CREDENTIALS env var is required for authentication."
        )

    rich.print(f"Attempting to create topic '{topic_name}' in project '{project_id}'...")
    publisher = PubSubPublisher(project_id=project_id, topic_name=topic_name)
    publisher.create_topic()

    rich.print(f"[green]Successfully initiated topic creation for '{topic_name}'.[/green]")


@pubsub_app.command()
def publish(
    topic: Annotated[
        str,
        typer.Option(help="The name of the topic to publish the message to."),
    ],
    *,
    message: Annotated[
        str | None,
        typer.Option(help="The message content to publish (as text or JSON string)."),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option(
            help="Path to a file containing the message content.",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ] = None,
    attributes: Annotated[
        str | None,
        typer.Option(
            "--attributes",
            help="Comma-separated key-value pairs in 'KEY1=VALUE1,KEY2=VALUE2' format.",
            rich_help_panel="Message Details",
        ),
    ] = None,
    project_id: Annotated[
        str,
        typer.Option(help="The Google Cloud Project ID.", rich_help_panel="GCP Configuration"),
    ],
    emulator: Annotated[
        bool,
        typer.Option(help="Use the Pub/Sub emulator.", rich_help_panel="Emulator Configuration"),
    ] = False,
    emulator_port: Annotated[
        int | None,
        typer.Option(
            help="Port of the Pub/Sub emulator. Required if --emulator is used.",
            rich_help_panel="Emulator Configuration",
        ),
    ] = None,
) -> None:
    """
    Publishes a message to a Pub/Sub topic.
    """
    # Validation for message source
    if not (message is not None) ^ (file is not None):
        raise typer.BadParameter("You must provide exactly one of --message or --file.")

    if emulator and emulator_port is None:
        raise typer.BadParameter("The --emulator-port is required when --emulator is set.")

    emulator_env = os.getenv("PUBSUB_EMULATOR_HOST", "")
    if emulator and not emulator_env:
        raise typer.BadParameter(
            "The PUBSUB_EMULATOR_HOST env var is required when --emulator is set."
        )

    application_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not emulator and not application_credentials:
        raise typer.BadParameter(
            "The GOOGLE_APPLICATION_CREDENTIALS env var is required for authentication."
        )

    message_attributes = {}
    if attributes:
        # Split the single string by commas
        attribute_list = attributes.split(",")
        for attr in attribute_list:
            attr = attr.strip()
            if not attr:
                continue
            try:
                key, value = attr.split("=", 1)
                message_attributes[key] = value
            except ValueError as e:
                raise typer.BadParameter(f"Attribute '{attr}' is not in 'KEY=VALUE' format.") from e

    message_content = ""
    if message:
        message_content = message
    elif file:
        message_content = file.read_text()

    data = {}
    try:
        data = json.loads(message_content)
    except json.decoder.JSONDecodeError:
        rich.print(
            f"It does not seem to be a json. We will send as text '{topic}' in project '{project_id}'..."
        )
        data = {"message": message}

    rich.print(f"Attempting to publish a message to topic '{topic}' in project '{project_id}'...")

    # --- Your message publishing logic goes here ---
    # Example:
    # from your_pubsub_client import PubSubManager
    # manager = PubSubManager(project_id, use_emulator=emulator, port=emulator_port)
    # manager.publish_message(topic, message_content)

    publisher = PubSubPublisher(project_id=project_id, topic_name=topic)
    publisher.publish(data=data, attributes=message_attributes)


@cli_app.command(name="help")
def show_help() -> None:
    """
    Shows a helpful guide with common use cases and examples.
    """
    explanation = """
    [bold cyan]StarConsumers CLI Usage Guide[/bold cyan]

    This tool is designed to get your application running with minimal effort.
    Here are some common scenarios and how to handle them:

    [bold]1. Basic App Usage (Auto-Discovery)[/bold]
    If your project has a standard structure (e.g., app/main.py with a variable named 'app'),
    you can simply run:
    [yellow]> starconsumers run[/yellow]

    [bold]2. Specifying a Path for the App[/bold]
    If your main file is in a different location:
    [yellow]> starconsumers run my_project/server.py[/yellow]

    [bold]3. Running Specific Tasks[/bold]
    To run only `task_a` and `task_b`:
    [yellow]> starconsumers run --tasks task_a --tasks task_b[/yellow]

    [bold]4. Running in Development Mode[/bold]
    To enable auto-reload on code changes, use the `--reload` flag:
    [yellow]> starconsumers run --reload[/yellow]

    [bold]5. Pub/Sub: Creating a Topic[/bold]
    To create a new topic in your GCP project:
    [yellow]> starconsumers pubsub create-topic my-new-topic --project-id gcp-project-123[/yellow]

    [bold]6. Pub/Sub: Publishing a Message from a File[/bold]
    Publish a JSON message from a file:
    [yellow]> starconsumers pubsub publish --topic my-topic --project-id gcp-project-123 --file ./payload.json[/yellow]

    [bold]7. Pub/Sub: Publishing a Message with Attributes[/bold]
    Attach key-value attributes to your message using a comma-separated string:
    [yellow]> starconsumers pubsub publish --topic orders --project-id gcp-123 --message '{"status": "shipped"}' --attributes "event_id=xyz-123,source=cli"[/yellow]

    [bold]8. Using the Pub/Sub Emulator[/bold]
    To use the local emulator for any pubsub command, add the --emulator flag and specify the port:
    [yellow]> starconsumers pubsub create-topic my-local-topic --project-id local-project --emulator --emulator-port 8085[/yellow]
    """
    rich.print(explanation)


if __name__ == "__main__":
    cli_app()
