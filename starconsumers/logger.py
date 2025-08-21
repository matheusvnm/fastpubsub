# --- 1. Context Management ---
# This remains the same, providing a thread-safe way to store context.

import json
import logging
import os
from re import I
import sys
import threading
from typing import Any


class ContextStore:
    """A thread-safe store for logging context."""
    def __init__(self):
        self._context = threading.local()

    def set(self, data: dict[str, Any]):
        self._context.data = data

    def get(self) -> dict[str, Any]:
        return getattr(self._context, 'data', {})

    def clear(self):
        self._context.data = {}

_context_store = ContextStore()


class ContextFilter(logging.Filter):
    """
    A logging filter that injects context from the ContextStore and the
    'extra' kwarg into each log record.
    """
    # These are the standard attributes of a LogRecord
    RESERVED_ATTRS = (
        'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
        'funcName', 'levelname', 'levelno', 'lineno', 'module',
        'msecs', 'message', 'msg', 'name', 'pathname', 'process',
        'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName',
        'taskName'
    )

    def filter(self, record: logging.LogRecord) -> bool:
        # Get the thread-local context (from .contextualize())
        thread_context = _context_store.get().copy()

        # Find any per-call context passed in the 'extra' dict.
        # These are added directly as attributes to the record.
        extra_context = {
            key: value for key, value in record.__dict__.items()
            if key not in self.RESERVED_ATTRS and key not in ('context',)
        }

        # Merge the two, with the per-call 'extra' context taking precedence.
        thread_context.update(extra_context)
        record.context = thread_context
        
        return True


class StarLogger(logging.Logger):
    """A custom logger class with a 'contextualize' method."""
    def contextualize(self, **kwargs):
        """
        A context manager to add temporary context to logs.
        
        Example:
            with logger.contextualize(trace_id="12345"):
                logger.info("This log will have the trace_id.")
        """
        class ContextManager:
            def __enter__(self):
                _context_store.set(kwargs)
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                _context_store.clear()
        
        return ContextManager()


class TextFormatter(logging.Formatter):
    """Formats logs as a human-readable string."""
    def format(self, record: logging.LogRecord) -> str:
        log_message = super().format(record)
        
        if hasattr(record, 'context') and record.context:
            context_text = " ".join(f"{k}={v}" for k, v in record.context.items())
            log_message += f" | {context_text}"
            
        return log_message

class JsonFormatter(logging.Formatter):
    """Formats logs as a JSON string."""
    def format(self, record: logging.LogRecord) -> str:
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
            **getattr(record, 'context', {}),
        }
        
        if record.exc_info:
            log_object["exception"] = self.formatException(record.exc_info)
 
        return json.dumps(log_object, indent=None, separators=(',', ':'))


class LoggerManager:
    """
    Manages an isolated logger for the 'starconsumers' library.
    """

    @staticmethod
    def setup(serialize: bool = False, level: str = "INFO"):
        """
        Enables and configures the StarConsumers logger.
        """
        logging.setLoggerClass(StarLogger)
        logger = logging.getLogger(__name__)
        logging.setLoggerClass(logging.Logger)

        logger.setLevel(level.upper())
        logger.propagate = False

        handler = logging.StreamHandler(sys.stdout)
        handler.addFilter(ContextFilter())

        formatter = JsonFormatter()
        if not serialize:
            fmt = "%(asctime)s | %(levelname)-8s | %(process)d:%(thread)d | %(module)s:%(funcName)s:%(lineno)d | %(message)s"
            formatter = TextFormatter(fmt)
            
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger


_log_level = str(os.getenv("STARCONSUMERS_LOG_LEVEL", "INFO"))
_log_serialize = bool(os.getenv("STARCONSUMERS_LOG_SERIALIZE", False))

logger: StarLogger = LoggerManager.setup(serialize=_log_serialize, level=_log_level)