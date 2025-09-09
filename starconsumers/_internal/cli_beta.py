"""Command-line interface."""

import asyncio
import json
from concurrent.futures import Future
from pathlib import Path
from typing import Annotated

import rich
import typer
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient

from starconsumers.concurrency import ProcessManager
from starconsumers.discover import discover_app

cli_app = typer.Typer(
    help="A CLI to discover and run StarConsumers applications and interact with Pub/Sub.",
    invoke_without_command=True,
    rich_markup_mode="markdown",
)

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
    app_str: str,
    tasks: Annotated[
        list[str] | None,
        typer.Option(
            "--task",
            help="Specify a task to run. Use this option multiple times for multiple tasks.",
        ),
    ] = [],
) -> None:
    """Run the StarConsumers application."""
    app = discover_app(app_str)

    subscribers = app.broker.subscribers
    if tasks:
        subscribers = {alias: sub for alias, sub in subscribers.items() if alias in tasks}

    if not subscribers:
        print("No subscribers found.")
        return

    process_manager = ProcessManager()

    wrapped_tasks = []
    for subscriber in subscribers.values():
        # This is a placeholder for the WrappedTask creation
        pass

    # process_manager.spawn(wrapped_tasks)

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        process_manager.terminate()


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
) -> None:
    """
    Creates a new Pub/Sub topic in the specified project.
    """
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
) -> None:
    """
    Publishes a message to a Pub/Sub topic.
    """
    if not (message is not None) ^ (file is not None):
        raise typer.BadParameter("You must provide exactly one of --message or --file.")

    message_attributes = {}
    if attributes:
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
    """
    rich.print(explanation)


def cli_main() -> None:
    cli_app()


if __name__ == "__main__":
    cli_main()
