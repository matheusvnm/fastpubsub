"""Command-line interface options."""

from typing import Annotated

import typer

from fastpubsub.cli.utils import LogLevels

CLIContext = typer.Context


AppArgument = Annotated[
    str,
    typer.Argument(
        ...,
        help="[module:FastPubSub]: The path to your application variable.",
        show_default=False,
    ),
]

AppNumWorkersOption = Annotated[
    int,
    typer.Option(
        "-w",
        "--workers",
        show_default=True,
        help="Run [workers] applications with process spawning. "
        "If set with --reload flag, it will be ignored.",
        envvar="FASTPUBSUB_WORKERS",
    ),
]

AppSelectedSubscribersOption = Annotated[
    list[str],
    typer.Option(
        "-s",
        "--subscribers",
        help="Specify the subscribers to run. If not selected, all will run.",
        envvar="FASTPUBSUB_SELECTED_SUBSCRIBERS",
    ),
]

AppHotReloadOption = Annotated[
    bool,
    typer.Option(
        "-r",
        "--reload",
        help="Restart app at directory files changes.",
        envvar="FASTPUBSUB_ENABLE_HOT_RELOAD",
    ),
]


AppHostOption = Annotated[
    str,
    typer.Option(
        "-h",
        "--host",
        help="The host to serve the application on. Use '0.0.0.0' for public access.",
        show_default=True,
        envvar="FASTPUBSUB_SERVER_HOST",
    ),
]

AppPortOption = Annotated[
    int,
    typer.Option(
        "-p",
        "--port",
        show_default=True,
        help="The port to serve the application on.",
        envvar="FASTPUBSUB_SERVER_PORT",
    ),
]

AppLogLevelOption = Annotated[
    LogLevels,
    typer.Option(
        "-l",
        "--log-level",
        case_sensitive=False,
        help="Set selected level for FastPubSub logger level.",
        show_default=False,
        envvar="FASTPUBSUB_LOG_LEVEL",
    ),
]


AppServerLogLevelOption = Annotated[
    LogLevels,
    typer.Option(
        "--server-log-level",
        case_sensitive=False,
        help="Set selected level for Uvicorn Server.",
        show_default=False,
        envvar="FASTPUBSUB_SERVER_LOG_LEVEL",
    ),
]


AppLogSerializeOption = Annotated[
    bool,
    typer.Option(
        "--log-serialize",
        help="Enables serialized logs in json format.",
        envvar="FASTPUBSUB_ENABLE_LOG_SERIALIZE",
    ),
]

AppLogColorizeOption = Annotated[
    bool,
    typer.Option(
        "--log-colors",
        help="Enables colorized logs.",
        envvar="FASTPUBSUB_ENABLE_LOG_COLORS",
    ),
]


AppVersionOption = Annotated[
    bool,
    typer.Option(
        "-v",
        "--version",
        is_eager=True,
        help="Show current platform, python and FastPubSub version.",
    ),
]
