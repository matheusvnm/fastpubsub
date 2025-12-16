"""Logging configuration for FastPubSub."""

import json
import logging
import logging.config
import os
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from copy import copy
from typing import Any, Literal, Self, cast

import click


class ContextStore:
    """A thread-safe store for logging context."""

    def __init__(self) -> None:
        """Initializes the ContextStore."""
        self._context: ContextVar[dict[str, str] | None] = ContextVar("context_store", default=None)

    def set(self, data: dict[str, Any]) -> Token[dict[str, str] | None]:
        """Sets or updates the context data.

        Args:
            data: The context data to set.
        """
        return self._context.set(data)

    def get(self) -> dict[str, Any]:
        """Gets the context data.

        Returns:
            The context data.
        """
        data = self._context.get()
        if not data:
            return {}

        return data.copy()

    def reset(self, token: Token[dict[str, str] | None]) -> None:
        """Reset the context data to its previous token."""
        self._context.reset(token)


_context_store = ContextStore()


class ContextFilter(logging.Filter):
    """A logging filter that injects context.

    The ContextStore and the 'extra' kwarg into each log record
    is used for this matter.

    """

    def filter(self, record: logging.LogRecord) -> bool:
        """Filters a log record.

        Args:
            record: The log record to filter.

        Returns:
            True if the record should be logged, False otherwise.
        """
        context = _context_store.get()
        record.context = context

        return True


class FastPubSubLogger(logging.Logger):
    """A custom logger class with a 'contextualize' method."""

    @contextmanager
    def contextualize(self, **kwargs: Any) -> Generator[Self]:
        """A context manager to add temporary context to logs.

        Example:
            with logger.contextualize(trace_id="12345"):
                logger.info("This log will have the trace_id.")
        """
        current_context = _context_store.get()
        current_context.update(kwargs)
        token = _context_store.set(current_context)
        try:
            yield self
        finally:
            _context_store.reset(token)


class DefaultFormatter(logging.Formatter):
    """Default formatter for a human-readable string."""

    level_name_colors = {
        logging.DEBUG: lambda level_name: click.style(str(level_name), fg="cyan"),
        logging.INFO: lambda level_name: click.style(str(level_name), fg="green"),
        logging.WARNING: lambda level_name: click.style(str(level_name), fg="yellow"),
        logging.ERROR: lambda level_name: click.style(str(level_name), fg="red"),
        logging.CRITICAL: lambda level_name: click.style(str(level_name), fg="bright_red"),
    }

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        use_colors: bool | None = None,
    ) -> None:
        """Initialized the default human-readable formatter."""
        self.use_colors = sys.stdout.isatty()
        if use_colors in (True, False):
            self.use_colors = use_colors

        super().__init__(fmt=fmt, datefmt=datefmt, style=style)

    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record.

        Args:
            record: The log record to format.

        Returns:
            The formatted log record.
        """
        recordcopy = copy(record)
        levelname = recordcopy.levelname
        separator = " " * (8 - len(recordcopy.levelname))
        if self.use_colors:
            levelname = self._get_colored_levelname(recordcopy.levelno, recordcopy.levelname)
            if "color_message" in recordcopy.__dict__:
                recordcopy.msg = recordcopy.__dict__["color_message"]
                recordcopy.__dict__["message"] = recordcopy.getMessage()

        recordcopy.__dict__["levelprefix"] = levelname + separator
        formatted_message = super().format(recordcopy)

        context = getattr(recordcopy, "context", {})
        for k, v in context.items():
            formatted_message += f"| {k}={v} "
        return formatted_message

    def _get_colored_levelname(self, level_num: int, level_name: str) -> str:
        get_colored_name: Callable[[str], str] = self.level_name_colors.get(
            level_num, lambda x: str(x)
        )
        return get_colored_name(level_name)


class JsonFormatter(logging.Formatter):
    """Formats logs as a JSON string."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record.

        Args:
            record: The log record to format.

        Returns:
            The formatted log record.
        """
        log_object = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "name": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread,
            **getattr(record, "context", {}),
        }

        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_object, indent=None, separators=(",", ":"))


def configure() -> FastPubSubLogger:
    """Enables and configures the FastPubSub logger."""
    LOGGING_LEVEL = os.getenv("FASTPUBSUB_LOG_LEVEL", "INFO")
    LOGGING_COLORIZE = bool(int(os.getenv("FASTPUBSUB_ENABLE_LOG_COLORS", 0)))
    LOGGING_SERIALIZE = bool(int(os.getenv("FASTPUBSUB_ENABLE_LOG_SERIALIZE", 0)))

    LOGGING_CONFIG: dict[str, Any] = {
        "version": 1,
        "formatters": {
            "fastpubsub_default": {
                "()": DefaultFormatter,
                "fmt": "%(asctime)s | %(levelprefix)s | "
                "%(process)d:%(thread)d | "
                "%(module)s:%(funcName)s:%(lineno)d | "
                "%(message)s ",
                "use_colors": LOGGING_COLORIZE,
            },
            "fastpubsub_json": {
                "()": JsonFormatter,
            },
        },
        "filters": {
            "fastpubsub_filter": {
                "()": ContextFilter,
            },
        },
        "handlers": {
            "fastpubsub_default": {
                "formatter": "fastpubsub_json" if LOGGING_SERIALIZE else "fastpubsub_default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "filters": ["fastpubsub_filter"],
            },
        },
        "loggers": {
            "fastpubsub": {
                "handlers": ["fastpubsub_default"],
                "level": LOGGING_LEVEL,
                "propagate": False,
            },
        },
        "disable_existing_loggers": False,
    }

    logging.setLoggerClass(FastPubSubLogger)
    logger = logging.getLogger("fastpubsub")
    if logger.hasHandlers():
        logger.handlers.clear()

    logging.config.dictConfig(LOGGING_CONFIG)
    return cast(FastPubSubLogger, logging.getLogger(__name__))


logger: FastPubSubLogger = configure()
