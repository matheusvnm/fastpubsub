"""Command-line interface utilities."""

import logging
import os
from enum import StrEnum

from fastpubsub.exceptions import FastPubSubCLIException


class LogLevels(StrEnum):
    """A class to represent log levels."""

    CRITICAL = "CRITICAL"
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"


LOGGING_LEVEL_MAP: dict[str, int] = {
    LogLevels.CRITICAL: logging.CRITICAL,
    LogLevels.FATAL: logging.FATAL,
    LogLevels.ERROR: logging.ERROR,
    LogLevels.WARNING: logging.WARNING,
    LogLevels.WARN: logging.WARNING,
    LogLevels.INFO: logging.INFO,
    LogLevels.DEBUG: logging.DEBUG,
}


def get_log_level(level: LogLevels | str | int) -> int:
    """Get the log level.

    Args:
        level: The log level to get. Can be an integer, a LogLevels enum value, or a string.

    Returns:
        The log level as an integer.

    """
    if isinstance(level, int):
        return level

    if isinstance(level, LogLevels):
        return LOGGING_LEVEL_MAP[level.value]

    if isinstance(level, str):  # pragma: no branch
        upper_level = level.upper()
        if upper_level in LOGGING_LEVEL_MAP:
            return LOGGING_LEVEL_MAP[upper_level]

    possible_values = list(LogLevels._value2member_map_.values())
    raise FastPubSubCLIException(
        f"Invalid value for '--log-level', it should be one of {possible_values}"
    )


def ensure_pubsub_credentials() -> None:
    """Ensures that the Pub/Sub credentials are set."""
    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not credentials and not emulator_host:
        raise FastPubSubCLIException(
            "You should set either of the environment variables for authentication: "
            "(GOOGLE_APPLICATION_CREDENTIALS, PUBSUB_EMULATOR_HOST)"
        )
