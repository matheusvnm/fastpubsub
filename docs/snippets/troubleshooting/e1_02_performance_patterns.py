import time

from fastpubsub import (
    BaseMiddleware,
    FastPubSub,
    Message,
    Middleware,
    PubSubBroker,
)
from fastpubsub.logger import logger


async def fast_operation(data: bytes) -> None:
    """Fast I/O-bound operation."""
    pass


# --8<-- [start:profiling_middleware]
class ProfilingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message):
        start = time.monotonic()
        result = await super().on_message(message)
        duration = (time.monotonic() - start) * 1000
        logger.info(f"Message {message.id} took {duration:.2f}ms")
        return result


# --8<-- [end:profiling_middleware]


# --8<-- [start:shutdown_timeout_broker]
broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    shutdown_timeout=30.0,  # Wait 30s for in-flight messages
    middlewares=[Middleware(ProfilingMiddleware)],
)
# --8<-- [end:shutdown_timeout_broker]

app = FastPubSub(broker)


# --8<-- [start:high_throughput_handler]
@broker.subscriber(
    alias="high-throughput-handler",
    topic_name="high-throughput",
    subscription_name="high-throughput-subscription",
    max_messages=500,  # Higher concurrency for I/O-bound tasks
)
async def high_throughput_handler(message: Message):
    await fast_operation(message.data)


# --8<-- [end:high_throughput_handler]


# --8<-- [start:low_memory_handler]
@broker.subscriber(
    alias="low-memory-handler",
    topic_name="memory-intensive",
    subscription_name="memory-intensive-subscription",
    max_messages=10,  # Lower for memory-intensive tasks
)
async def low_memory_handler(message: Message):
    pass


# --8<-- [end:low_memory_handler]


# BAD: Memory leak example (commented out)
# all_messages = []
#
# @broker.subscriber(...)
# async def bad_handler(message: Message):
#     all_messages.append(message)  # Never cleared!


async def process_in_order(user_id: str, data: bytes) -> None:
    """Process messages in order for a user."""
    pass


# --8<-- [start:ordered_handler]
@broker.subscriber(
    alias="ordered-handler",
    topic_name="ordered-events",
    subscription_name="ordered-events-subscription",
    enable_message_ordering=True,
)
async def ordered_handler(message: Message):
    user_id = message.attributes.get("user_id", "unknown-user")
    await process_in_order(user_id, message.data)


# --8<-- [end:ordered_handler]
