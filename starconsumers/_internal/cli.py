import json
import os
from concurrent.futures import Future
from pathlib import Path
from typing import Annotated

import rich
import typer
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient

from starconsumers._internal.runner import AppConfiguration, ApplicationRunner, ServerConfiguration

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
    app_path: Annotated[
        str,
        typer.Argument(
            help="Path to the app variable containing the StarConsumers application in format module:app."
        ),
    ],
    *,
    tasks: Annotated[
        list[str] | None,
        typer.Option(
            "--task",
            help="Specify a task to run. Use this option multiple times for multiple tasks.",
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
) -> None:
    """
    Run a StarConsumers application using Uvicorn.

    This tool simplifies running applications by letting the user specify
    which consumers tasks to run using the --tasks option.

    The user can also pass no tasks flag to run all of them.
    """

    app_configuration = AppConfiguration(
        path=app_path,
        tasks=tasks if tasks else [],
    )

    server_configuration = ServerConfiguration(
        host=host,
        port=port,
    )

    application_runner = ApplicationRunner()
    application_runner.run(app_configuration, server_configuration)


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

    rich.print(f"Attempting to create topic '{topic_name}' in project '{project_id}'")

    try:
        name = PublisherClient.topic_path(project=project_id, topic=topic_name)

        client = PublisherClient()
        client.create_topic(name=name)
        rich.print(f"Successfully created topic '{name}'.")
    except AlreadyExists:
        rich.print(f"The topic '{topic_name}' already exists.")
    except Exception as e:
        rich.print(f"Topic creation failed due to {e}.")


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
        data = {"message": message}
        rich.print(
            "It does not seem to be a json."
            f"We will send as text to '{topic}' in project '{project_id}'..."
        )

    rich.print(f"Attempting to publish a message to topic '{topic}' in project '{project_id}'...")
    client = PublisherClient()
    try:
        topic_name = PublisherClient.topic_path(project=project_id, topic=topic)
        encoded_data = json.dumps(data).encode()
        future: Future[str] = client.publish(
            topic=topic_name, data=encoded_data, **message_attributes
        )
        message_id = future.result()
        rich.print(f"Message published for topic '{topic_name}' with id '{message_id}'")
        rich.print(f"We sent {data} with metadata {message_attributes}")
    except Exception as e:
        rich.print(f"Publisher failed due to {e}.")


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
    [yellow]> starconsumers run app.main:app [/yellow]

    [bold]2. Running Specific Tasks[/bold]
    To run only `task_a` and `task_b`:
    [yellow]> starconsumers run app.main:app --task task_a --task task_b[/yellow]

    [bold]3. Pub/Sub: Creating a Topic[/bold]
    To create a new topic in your GCP project:
    [yellow]> starconsumers pubsub create-topic my-new-topic --project-id gcp-project-123[/yellow]

    [bold]4. Pub/Sub: Publishing a Message from a File[/bold]
    Publish a JSON message from a file:
    [yellow]> starconsumers pubsub publish --topic my-topic --project-id gcp-project-123 --file ./payload.json[/yellow]

    [bold]5. Pub/Sub: Publishing a Message with Attributes[/bold]
    Attach key-value attributes to your message using a comma-separated string:
    [yellow]> starconsumers pubsub publish --topic orders --project-id gcp-123 --message '{"status": "shipped"}' --attributes "event_id=xyz-123,source=cli"[/yellow]

    [bold]6. Using the Pub/Sub Emulator[/bold]
    To use the local emulator for any pubsub command, add the --emulator flag and specify the port:
    [yellow]> starconsumers pubsub create-topic my-local-topic --project-id local-project --emulator --emulator-port 8085[/yellow]
    """
    rich.print(explanation)


def cli_main() -> None:
    cli_app()


if __name__ == "__main__":
    cli_main()
