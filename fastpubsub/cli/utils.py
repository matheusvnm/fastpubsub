"""Command-line interface utilities."""

from __future__ import annotations

import os
from enum import StrEnum
from typing import TYPE_CHECKING

from fastpubsub.exceptions import FastPubSubCLIException

if TYPE_CHECKING:
    from fastpubsub.applications import FastPubSub


class LogLevels(StrEnum):
    """A class to represent log levels."""

    CRITICAL = "critical"
    FATAL = "fatal"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"


def ensure_pubsub_credentials() -> None:
    """Ensures that the Pub/Sub credentials are set."""
    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not credentials and not emulator_host:
        raise FastPubSubCLIException(
            "You should set either of the "
            "environment variables for authentication: "
            "(GOOGLE_APPLICATION_CREDENTIALS, PUBSUB_EMULATOR_HOST)"
        )


def import_app(app_path: str) -> FastPubSub:
    """Import and validate a FastPubSub application from a module path.

    Args:
        app_path: The application path in "module:attribute" format.

    Returns:
        The FastPubSub application instance.

    Raises:
        FastPubSubCLIException: If the import fails or the object
            is not a FastPubSub instance.
    """
    from fastpubsub.applications import FastPubSub as _FastPubSub

    if ":" not in app_path:
        raise FastPubSubCLIException(
            f"Invalid app path '{app_path}'. "
            "Expected format: module:attribute "
            "(e.g., myapp.main:app)"
        )

    module_str, _ = app_path.rsplit(":", 1)

    try:
        import uvicorn.importer

        app = uvicorn.importer.import_from_string(app_path)
    except ModuleNotFoundError as exc:
        raise FastPubSubCLIException(
            f"Could not find module '{module_str}'. "
            "Check the module path and ensure it is "
            "importable from the current directory."
        ) from exc
    except ImportError as exc:
        raise FastPubSubCLIException(
            f"Failed to import '{app_path}': {exc}"
        ) from exc

    if not isinstance(app, _FastPubSub):
        raise FastPubSubCLIException(
            f"The object at '{app_path}' is not a "
            f"FastPubSub instance (got {type(app).__name__})."
        )

    return app
