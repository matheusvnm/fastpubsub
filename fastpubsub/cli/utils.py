"""Command-line interface utilities."""

import os
from enum import StrEnum

from fastpubsub.exceptions import FastPubSubCLIException


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
            "You should set either of the environment variables for authentication: "
            "(GOOGLE_APPLICATION_CREDENTIALS, PUBSUB_EMULATOR_HOST)"
        )
