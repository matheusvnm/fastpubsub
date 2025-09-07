"""Discover StarConsumers application."""
import importlib
from typing import Any

from starconsumers.app import StarConsumers


def discover_app(app_str: str) -> StarConsumers:
    """Discover and return the StarConsumers app instance."""
    module_str, app_instance_str = app_str.split(":")
    module = importlib.import_module(module_str)
    app_instance = getattr(module, app_instance_str)
    if not isinstance(app_instance, StarConsumers):
        raise TypeError(f"{app_instance_str} is not a StarConsumers instance.")
    return app_instance
