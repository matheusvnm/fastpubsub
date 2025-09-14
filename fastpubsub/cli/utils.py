import logging
from collections import defaultdict
from enum import StrEnum
import os

from fastpubsub.exceptions import StarConsumersCLIException, StarConsumersException


class LogLevels(StrEnum):
    """A class to represent log levels.
    """

    critical = "CRITICAL"
    fatal = "FATAL"
    error = "ERROR"
    warning = "WARNING"
    warn = "WARN"
    info = "INFO"
    debug = "DEBUG"


LOGGING_LEVEL_MAP: dict[str, int] = {
    LogLevels.critical: logging.CRITICAL,
    LogLevels.fatal: logging.FATAL,
    LogLevels.error: logging.ERROR,
    LogLevels.warning: logging.WARNING,
    LogLevels.warn: logging.WARNING,
    LogLevels.info: logging.INFO,
    LogLevels.debug: logging.DEBUG,
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
        return LOGGING_LEVEL_MAP[level.lower()]

    possible_values = list(LogLevels._value2member_map_.values())
    raise StarConsumersCLIException(f"Invalid value for '--log-level', it should be one of {possible_values}")



class APMProviders(StrEnum):
    """A class to represent the possible APM providers.
    """

    NOOP = "NOOP"
    NEWRELIC = "NEWRELIC"



def ensure_pubsub_credentials() -> None:
    credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not credentials and not emulator_host:
        raise StarConsumersCLIException(
            "You should set either of the environment variables for authentication:"
            " (GOOGLE_APPLICATION_CREDENTIALS, PUBSUB_EMULATOR_HOST)"
        )

