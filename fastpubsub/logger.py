"""Logging configuration for FastPubSub."""

import json
import logging
import os
import sys
import threading
from contextlib import contextmanager
from typing import Any, cast


class ContextStore:
    """A thread-safe store for logging context."""

    def __init__(self) -> None:
        """Initializes the ContextStore."""
        self._context = threading.local()

    def set(self, data: dict[str, Any]) -> None:
        """Sets the context data.

        Args:
            data: The context data to set.
        """
        self._context.data = data

    def get(self) -> dict[str, Any]:
        """Gets the context data.

        Returns:
            The context data.
        """
        return getattr(self._context, "data", {})

    def clear(self) -> None:
        """Clears the context data."""
        self._context.data = {}


_context_store = ContextStore()


class ContextFilter(logging.Filter):
    """A logging filter that injects context.

    The ContextStore and the 'extra' kwarg into each log record
    is used for this matter.

    """

    # These are the standard attributes of a LogRecord
    RESERVED_ATTRS = (
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """Filters a log record.

        Args:
            record: The log record to filter.

        Returns:
            True if the record should be logged, False otherwise.
        """
        thread_context = _context_store.get().copy()

        extra_context = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self.RESERVED_ATTRS and key not in ("context",)
        }

        # Merge the two, with the per-call 'extra' context taking precedence.
        thread_context.update(extra_context)
        record.context = thread_context

        return True


class FastPubSubLogger(logging.Logger):
    """A custom logger class with a 'contextualize' method."""

    @contextmanager
    def contextualize(self, **kwargs: Any) -> Any:
        """A context manager to add temporary context to logs.

        Example:
            with logger.contextualize(trace_id="12345"):
                logger.info("This log will have the trace_id.")
        """
        _context_store.set(kwargs)
        yield
        _context_store.clear()


class TextFormatter(logging.Formatter):
    """Formats logs as a human-readable string."""

    def format(self, record: logging.LogRecord) -> str:
        """Formats a log record.

        Args:
            record: The log record to format.

        Returns:
            The formatted log record.
        """
        log_message = super().format(record)

        if hasattr(record, "context") and record.context:
            context_text = " ".join(f"{k}={v}" for k, v in record.context.items() if v)
            if context_text:
                log_message += f" | {context_text}"

        return log_message


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


def setup_logger() -> FastPubSubLogger:
    """Enables and configures the FastPubSub logger."""
    # V2: Add colorized logs
    log_level = int(os.getenv("FASTPUBSUB_LOG_LEVEL", logging.INFO))
    log_serialize = bool(int(os.getenv("FASTPUBSUB_ENABLE_LOG_SERIALIZE", 0)))

    logging.setLoggerClass(FastPubSubLogger)
    logger = logging.getLogger("fastpubsub")
    logging.setLoggerClass(logging.Logger)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(log_level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(ContextFilter())

    formatter: logging.Formatter = JsonFormatter()
    if not log_serialize:
        fmt = (
            "%(asctime)s | %(levelname)-8s "
            "| %(process)d:%(thread)d "
            "| %(module)s:%(funcName)s:%(lineno)d "
            "| %(message)s"
        )
        formatter = TextFormatter(fmt)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return cast(FastPubSubLogger, logger)


logger: FastPubSubLogger = setup_logger()
