import os
from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from functools import cache
from typing import Any

from starconsumers.exceptions import StarConsumersException
from starconsumers.logger import logger


class ApmProvider(ABC):
    """Abstract base class defining the contract for any APM provider."""

    @abstractmethod
    def initialize(self) -> None:
        """
        Initializes the APM agent if it's not already running.
        This is for environments without an auto-starting wrapper.
        """
        pass

    @abstractmethod
    @contextmanager
    def background_transaction(self, name: str) -> Generator[Any]:
        """Decorator for a background transaction (a top-level trace)."""
        pass

    @abstractmethod
    @contextmanager
    def span(self, name: str) -> Generator[Any]:
        """Decorator for a span (a nested operation within a transaction)."""
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
    def record_custom_event(self, event_type: str, params: dict[str, str]) -> None:
        """Records a custom event with associated attributes."""
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

    def initialize(self) -> None:
        pass

    @contextmanager
    def background_transaction(self, _: str) -> Generator[Any]:
        yield

    @contextmanager
    def span(self, _: str) -> Generator[Any]:
        yield

    def set_distributed_trace_context(self, _: dict[str, str]) -> None:
        pass

    def get_distributed_trace_context(self) -> dict[str, str]:
        return {}

    def record_custom_event(self, _: str, __: dict[str, str]) -> None:
        pass

    def get_trace_id(self) -> str | None:
        return ""

    def get_span_id(self) -> str | None:
        return ""

    def active(self) -> bool:
        return False


class NewRelicProvider(ApmProvider):
    """APM provider for New Relic."""

    def __init__(self) -> None:
        try:
            import newrelic.agent

            self._agent = newrelic.agent
        except ModuleNotFoundError as e:
            logger.exception("No newrelic module found.")
            raise StarConsumersException(
                "No newrelic module found. "
                "Please install it using 'pip install starconsumers[newrelic]'."
            ) from e

    def initialize(self) -> None:
        """Initializes and registers the agent if not already active."""
        logger.info("Performing New Relic agent initialization.")
        try:
            self._agent.initialize()
            self._agent.register_application(timeout=1.0)
            logger.info("New Relic initialization and registration successful.")
        except Exception:
            logger.exception("Failed to initialize New Relic.")

    @contextmanager
    def background_transaction(self, name: str) -> Generator[Any]:
        app = self._agent.application(activate=False)
        with self._agent.BackgroundTask(application=app, name=name):
            yield

    @contextmanager
    def span(self, name: str) -> Generator[Any]:
        with self._agent.FunctionTrace(name=name):
            yield

    def set_distributed_trace_context(self, headers: dict[str, str]) -> None:
        """Sets the distributed trace for headers"""
        if not headers:
            return

        context: list[tuple[str, str]] = []
        for k, v in headers.items():
            context.append((str(k).lower(), str(v)))

        self._agent.accept_distributed_trace_headers(context, transport_type="Queue")

    def get_distributed_trace_context(self) -> dict[str, str]:
        """Get the distributed trace for headers from current context"""

        headers: list[tuple[str, str]] = []
        self._agent.insert_distributed_trace_headers(headers)
        return dict(headers)

    def record_custom_event(self, event_type: str, params: dict[str, str]) -> None:
        """Records a New Relic custom event. Must be called within a transaction."""
        try:
            self._agent.record_custom_event(event_type, params)
        except Exception:
            logger.exception("Failed to record New Relic custom event", stacklevel=5)

    def get_trace_id(self) -> str | None:
        trace_id = self._agent.current_trace_id()
        if trace_id:
            return str(trace_id)
        return None

    def get_span_id(self) -> str | None:
        span_id = self._agent.current_span_id()
        if span_id:
            return str(span_id)
        return None

    def active(self) -> bool:
        application = self._agent.application(activate=False)
        return bool(application) and bool(application.active)


@cache
def get_apm_provider() -> ApmProvider:
    name = os.getenv("STARCONSUMERS_APM_PROVIDER", "")
    name = name.lower()

    provider_map = {
        "newrelic": NewRelicProvider,
    }
    provider_cls = provider_map.get(name, NoOpProvider)
    provider = provider_cls()

    logger.debug(f"The observability method choosen is: {name} {provider_cls.__name__}")
    return provider
