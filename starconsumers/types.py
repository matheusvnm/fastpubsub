

from collections.abc import Callable
from typing import Any


DecoratedCallable = Callable[[Any], Any]
SubscribedCallable = Callable[[DecoratedCallable], DecoratedCallable]
