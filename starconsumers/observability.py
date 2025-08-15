from contextlib import contextmanager
import os
import sys

from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional


class ApmProvider(ABC):
    """Abstract base class defining the contract for any APM provider."""
    
    @abstractmethod
    def initialize(self):
        """
        Initializes the APM agent if it's not already running.
        This is for environments without an auto-starting wrapper.
        """
        pass

    @abstractmethod
    def background_transaction(self, name: str):
        """Decorator for a background transaction (a top-level trace)."""
        pass

    @abstractmethod
    def span(self, name: str):
        """Decorator for a span (a nested operation within a transaction)."""
        pass

    @abstractmethod
    def set_distributed_trace_context(self, headers: dict):
        """Sets the current trace context from incoming distributed trace headers."""
        pass
    
    @abstractmethod
    def get_distributed_trace_context(self) -> dict:
        """Gets the current trace context as headers for downstream propagation."""
        pass

    @abstractmethod
    def record_custom_event(self, event_type: str, params: dict):
        """Records a custom event with associated attributes."""
        pass
    
    @abstractmethod
    def get_trace_id(self) -> str:
        """Gets the trace id from the current transaction."""
        pass

    @abstractmethod
    def get_span_id(self) -> str:
        """Gets the span id from the current transaction."""
        pass



class _NoOpContextDecorator:
    """A helper that works as a no-op decorator and context manager."""
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper

    def __enter__(self):
        # Required for context manager, does nothing.
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Required for context manager, does nothing.
        pass

class NoOpProvider(ApmProvider):
    """A provider that performs no operations."""
    def initialize(self):
        pass # Nothing to do

    def background_transaction(self, name: str):
        return _NoOpContextDecorator()

    def span(self, name: str):
        return _NoOpContextDecorator()

    def set_distributed_trace_context(self, headers: dict):
        pass

    def get_distributed_trace_context(self) -> dict:
        return {}
        
    def record_custom_event(self, event_type: str, params: dict):
        pass

    def get_trace_id(self) -> str:
        return ""

    def get_span_id(self) -> str:
        return ""

class NewRelicProvider(ApmProvider):
    """APM provider for New Relic."""
    def __init__(self):
        import newrelic.agent
        self._agent = newrelic.agent

    def initialize(self):
        """Initializes and registers the agent if not already active."""
        print("New Relic agent not running. Performing initialization...")
        try:
            self._agent.initialize()
            self._agent.register_application(timeout=10.0)
            print("New Relic initialization and registration successful.")
        except Exception as e:
            print(f"Error during New Relic initialization: {e}", file=sys.stderr)

    @contextmanager
    def background_transaction(self, name: str):
        app = self._agent.application(activate=False)
        with self._agent.BackgroundTask(application=app, name=name):
            yield

    def span(self, name: str):
        return self._agent.function_trace(name=name)

    def set_distributed_trace_context(self, headers: dict):
        """Sets the distributed trace for headers"""
        if not headers:
            return 

        context: list[tuple[str, str]] = list()
        for k, v in headers.items():
            context.append((str(k).lower(), str(v)))

        self._agent.accept_distributed_trace_headers(context, transport_type='Queue')

    def get_distributed_trace_context(self) -> dict:
        """Get the distributed trace for headers from current context"""
        
        headers: list[tuple] = []
        self._agent.insert_distributed_trace_headers(headers)
        return dict(headers)

    def record_custom_event(self, event_type: str, params: dict):
        """Records a New Relic custom event. Must be called within a transaction."""
        try:
            self._agent.record_custom_event(event_type, params)
        except Exception as e:
            print(f"Failed to record New Relic custom event: {e}", file=sys.stderr)

    def get_trace_id(self) -> str:
        return self._agent.current_trace_id()
    
    def get_span_id(self) -> str:
        return self._agent.current_span_id()

class ApmFactory:
    """Factory for creating and managing the singleton APM provider instance."""
    _instance: Optional[ApmProvider] = None

    @classmethod
    def _get_provider(cls) -> ApmProvider:
        """
        Retrieves the APM provider instance. Initializes it on first call.
        Can be configured programmatically or via 'STARCONSUMERS_APM_PROVIDER' environment variable.
        """
        if cls._instance is None:
            name = os.getenv("STARCONSUMERS_APM_PROVIDER", "")
            name = name.lower()
            
            provider_map = {
                'newrelic': NewRelicProvider,
            }
            ProviderClass = provider_map.get(name, NoOpProvider)
            cls._instance = ProviderClass()

            print(f"Initializing observability method: {name} {ProviderClass.__name__}")
            cls._instance.initialize()

        return cls._instance


apm: ApmProvider = ApmFactory._get_provider()