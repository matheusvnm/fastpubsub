"""Observability utilities."""

import os
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from functools import cache
from types import TracebackType
from typing import Any

from fastpubsub.exceptions import FastPubSubException
from fastpubsub.logger import logger

try:
    import newrelic.agent

    _new_relic_agent = newrelic.agent
except ModuleNotFoundError:
    _new_relic_agent = None
    pass


class ApmProvider(ABC):
    """Abstract base class defining the contract for any APM provider."""

    @abstractmethod
    def start(self) -> None:
        """Initializes the APM agent if it's not already running.

        This is for environments without an auto-starting wrapper.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdowm the APM agent if it's running.

        This is for environments without an auto-starting wrapper.
        """
        pass

    @abstractmethod
    @contextmanager
    def start_trace(self, name: str, context: dict[str, str] | None = None) -> Generator[Any]:
        """Decorator for a trace (a top-level trace)."""
        pass

    @abstractmethod
    @contextmanager
    def start_span(self, name: str) -> Generator[Any]:
        """Decorator for a span (a nested operation within a trace)."""
        pass

    @abstractmethod
    def set_distributed_trace_context(self, headers: dict[str, str]) -> None:
        """Sets the current trace context from incoming distributed trace headers."""
        pass

    @abstractmethod
    def get_distributed_trace_context(self) -> dict[str, str]:
        """Gets the current trace context as headers for downstream propagation."""
        pass

    @abstractmethod
    def report_custom_event(self, event_name: str, params: dict[str, str]) -> None:
        """Reports a custom event with associated attributes."""
        pass

    @abstractmethod
    def report_log_record(
        self, message: str, level: str, timestamp: float, attributes: dict[str, str] | None = None
    ) -> None:
        """Reports a log message with associated attributes."""
        pass

    @abstractmethod
    def report_exception(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Reports a exception associated with the trace."""
        pass

    @abstractmethod
    def add_custom_metric(self, metric_name: str, value: int | float | dict[str, str]) -> None:
        """Reports a custom metric associated with the trace."""
        pass

    @abstractmethod
    def get_trace_id(self) -> str | None:
        """Gets the trace id from the current transaction."""
        pass

    @abstractmethod
    def get_span_id(self) -> str | None:
        """Gets the span id from the current transaction."""
        pass

    @abstractmethod
    def active(self) -> bool:
        """Returns if the provider is active and ready."""
        return False


class NoOpProvider(ApmProvider):
    """A provider that performs no operations."""

    def start(self) -> None:
        """Starts the APM provider."""
        return None

    def shutdown(self) -> None:
        """Shuts down the APM provider."""
        return None

    @contextmanager
    def start_trace(self, name: str, context: dict[str, str] | None = None) -> Generator[Any]:
        """Starts a trace.

        Args:
            name: The name of the trace.
            context: The distributed trace context.
        """
        yield

    @contextmanager
    def start_span(self, name: str) -> Generator[Any]:
        """Starts a span.

        Args:
            name: The name of the span.
        """
        yield

    def set_distributed_trace_context(self, headers: dict[str, str]) -> None:
        """Sets the distributed trace context.

        Args:
            headers: The distributed trace headers.
        """
        return None

    def get_distributed_trace_context(self) -> dict[str, str]:
        """Gets the distributed trace context.

        Returns:
            The distributed trace context.
        """
        return {}

    def report_custom_event(self, event_name: str, params: dict[str, str]) -> None:
        """Reports a custom event.

        Args:
            event_name: The name of the event.
            params: The event parameters.
        """
        return None

    def report_log_record(
        self, message: str, level: str, timestamp: float, attributes: dict[str, str] | None = None
    ) -> None:
        """Reports a log record.

        Args:
            message: The log message.
            level: The log level.
            timestamp: The timestamp of the log record.
            attributes: A dictionary of attributes.
        """
        return None

    def report_exception(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Reports an exception.

        Args:
            exc_type: The type of the exception.
            exc_value: The exception value.
            traceback: The traceback.
            attributes: A dictionary of attributes.
        """
        return None

    def add_custom_metric(self, metric_name: str, value: int | float | dict[str, str]) -> None:
        """Adds a custom metric.

        Args:
            metric_name: The name of the metric.
            value: The value of the metric.
        """
        return None

    def get_trace_id(self) -> str | None:
        """Gets the trace ID.

        Returns:
            The trace ID.
        """
        return ""

    def get_span_id(self) -> str | None:
        """Gets the span ID.

        Returns:
            The span ID.
        """
        return ""

    def active(self) -> bool:
        """Checks if the provider is active.

        Returns:
            True if the provider is active, False otherwise.
        """
        return False


class NewRelicProvider(ApmProvider):
    """APM provider for New Relic."""

    def __init__(self) -> None:
        """Initializes the NewRelicProvider."""
        if not _new_relic_agent:
            raise FastPubSubException(
                "No newrelic agent found. "
                "Please install it using 'pip install fastpubsub[newrelic]'."
            )

        self._agent = _new_relic_agent

    def start(self) -> None:
        """Initializes and registers the agent if not already active."""
        logger.info(f"Performing New Relic agent initialization for process [{os.getpid()}].")
        try:
            if self.active():
                logger.warning("The New Relic is already active.")
                return

            self._agent.initialize()
            self._agent.register_application(timeout=5.0)
            logger.info(
                f"New Relic initialization and registration successful for process [{os.getpid()}]."
            )
        except Exception:
            logger.exception(
                f"Failed to initialize New Relic for process [{os.getpid()}].", stacklevel=5
            )

    def shutdown(self) -> None:
        """Shuts down the New Relic agent."""
        logger.info(f"Performing New Relic agent shutdown for process [{os.getpid()}].")
        try:
            self._agent.shutdown_agent()
        except Exception:
            logger.exception(
                f"Failed to shutdown New Relic for process [{os.getpid()}].",
                stacklevel=5,
            )

    @contextmanager
    def start_trace(self, name: str, context: dict[str, str] | None = None) -> Generator[Any]:
        """Starts a trace.

        Args:
            name: The name of the trace.
            context: The distributed trace context.
        """
        app = self._agent.application(activate=False)
        with self._agent.BackgroundTask(application=app, name=name) as transaction:
            if context and isinstance(context, dict):
                self.set_distributed_trace_context(headers=context)

            yield transaction

    @contextmanager
    def start_span(self, name: str) -> Generator[Any]:
        """Starts a span.

        Args:
            name: The name of the span.
        """
        with self._agent.FunctionTrace(name=name) as function:
            yield function

    def set_distributed_trace_context(self, headers: dict[str, str]) -> None:
        """Sets the distributed trace context.

        Args:
            headers: The distributed trace headers.
        """
        if not headers:
            return

        context: list[tuple[str, str]] = []
        for k, v in headers.items():
            context.append((str(k).lower(), str(v)))
        self._agent.accept_distributed_trace_headers(context, transport_type="Queue")

    def get_distributed_trace_context(self) -> dict[str, str]:
        """Gets the distributed trace context.

        Returns:
            The distributed trace context.
        """
        headers: list[tuple[str, str]] = []
        self._agent.insert_distributed_trace_headers(headers)
        return dict(headers)

    def report_custom_event(self, event_name: str, params: dict[str, str]) -> None:
        """Reports a custom event.

        Args:
            event_name: The name of the event.
            params: The event parameters.
        """
        try:
            self._agent.record_custom_event(event_type=event_name, params=params)
        except Exception:
            logger.exception("Failed to record New Relic custom event", stacklevel=5)

    def report_log_record(
        self, message: str, level: str, timestamp: float, attributes: dict[str, str] | None = None
    ) -> None:
        """Reports a log record.

        Args:
            message: The log message.
            level: The log level.
            timestamp: The timestamp of the log record.
            attributes: A dictionary of attributes.
        """
        self._agent.record_log_event(
            message=message, level=level, timestamp=timestamp, attributes=attributes
        )

    def report_exception(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        """Reports an exception.

        Args:
            exc_type: The type of the exception.
            exc_value: The exception value.
            traceback: The traceback.
            attributes: A dictionary of attributes.
        """
        error = (
            exc_type,
            exc_value,
            traceback,
        )
        self._agent.notice_error(error=error, attributes=attributes)

    def add_custom_metric(self, metric_name: str, value: int | float | dict[str, str]) -> None:
        """Adds a custom metric.

        Args:
            metric_name: The name of the metric.
            value: The value of the metric.
        """
        self._agent.record_custom_metric(name=metric_name, value=value)

    def get_trace_id(self) -> str | None:
        """Gets the trace ID.

        Returns:
            The trace ID.
        """
        trace_id = self._agent.current_trace_id()
        if trace_id:
            return str(trace_id)
        return None

    def get_span_id(self) -> str | None:
        """Gets the span ID.

        Returns:
            The span ID.
        """
        span_id = self._agent.current_span_id()
        if span_id:
            return str(span_id)
        return None

    def active(self) -> bool:
        """Checks if the provider is active.

        Returns:
            True if the provider is active, False otherwise.
        """
        application = self._agent.application(activate=False)
        return bool(application) and bool(application.active)


PROVIDER_MAP: dict[str, type[ApmProvider]] = {
    "newrelic": NewRelicProvider,
}


@cache
def get_apm_provider(provider_name: str | None = None) -> ApmProvider:
    """Gets the APM provider.

    Args:
        provider_name: The name of the provider.

    Returns:
        The APM provider.
    """
    name = provider_name or os.getenv("FASTPUBSUB_APM_PROVIDER")
    name = name.lower() if isinstance(name, str) else ""

    provider_cls = PROVIDER_MAP.get(name, NoOpProvider)
    provider = provider_cls()

    logger.debug(f"The observability method choosen is: {name} {provider_cls.__name__}")
    return provider
