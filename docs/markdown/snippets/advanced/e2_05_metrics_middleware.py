"""Title: Metrics and Observability Middleware

Demonstrates middleware for collecting Prometheus metrics.
"""

import time
from typing import Any

from prometheus_client import Counter, Histogram

from fastpubsub import BaseMiddleware, Message

# --8<-- [start:metrics_middleware]
MESSAGES_PROCESSED = Counter(
    "pubsub_messages_processed_total",
    "Total messages processed",
    ["subscriber", "status"],
)
PROCESSING_TIME = Histogram(
    "pubsub_processing_seconds",
    "Message processing time",
    ["subscriber"],
)


class MetricsMiddleware(BaseMiddleware):
    def __init__(self, subscriber_name: str):
        self.subscriber_name = subscriber_name

    async def on_message(self, message: Message) -> Any:
        start = time.monotonic()
        status = "success"

        try:
            result = await super().on_message(message)
            return result
        except Exception:
            status = "error"
            raise
        finally:
            duration = time.monotonic() - start
            MESSAGES_PROCESSED.labels(
                subscriber=self.subscriber_name, status=status
            ).inc()
            PROCESSING_TIME.labels(subscriber=self.subscriber_name).observe(duration)
# --8<-- [end:metrics_middleware]
