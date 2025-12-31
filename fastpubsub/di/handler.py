"""Handler class for wrapping async handlers with DI injection."""

import inspect
from dataclasses import dataclass
from typing import get_args, get_origin

from fast_depends import inject
from fast_depends.dependencies import Dependant
from fast_depends.library import CustomField

from fastpubsub.types import AsyncCallable


def _has_custom_field_annotation(param: inspect.Parameter) -> bool:
    """Check if a parameter has a CustomField or Dependant annotation."""
    annotation = param.annotation
    if annotation is inspect.Parameter.empty:
        return False

    if get_origin(annotation) is not None:
        args = get_args(annotation)
        for arg in args:
            if isinstance(arg, (CustomField, Dependant)):
                return True
    return False


@dataclass
class Handler:
    """Wraps an async handler with DI injection and extracted metadata.

    The Handler class combines:
    - The injected target function (wrapped with fast_depends)
    - Metadata about the handler's parameters for auto-unwrapping

    Attributes:
        target: The inject(func) wrapped function ready for DI.
        unannotated_param_names: Set of parameter names without annotations.
    """

    def __init__(self, target: AsyncCallable):
        """Initialize the Handler with a target async function.

        Args:
            target: The async callable to wrap with dependency injection.
        """
        self.name = getattr(target, "__name__", "")

        self.target = inject(target)
        self.unannotated_param_names: set[str] = set()

        sig = inspect.signature(target)
        for name, param in sig.parameters.items():
            if param.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            ):
                if not _has_custom_field_annotation(param):
                    self.unannotated_param_names.add(name)
