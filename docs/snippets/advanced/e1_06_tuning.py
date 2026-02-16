from fastpubsub import FastPubSub, Message, Middleware, PubSubBroker
from fastpubsub.middlewares import GZipMiddleware

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def fast_async_operation(data: bytes) -> None:
    """Fast async operation."""
    pass


async def complex_ml_inference(data: bytes) -> None:
    """Complex ML inference."""
    pass


async def send_notification(data: bytes) -> None:
    """Send notification."""
    pass


async def handle_event(data: bytes) -> None:
    """Handle event."""
    pass


# --8<-- [start:high_concurrency]
@broker.subscriber(
    alias="high-throughput",
    topic_name="high-events",
    subscription_name="high-events-subscription",
    max_messages=500,
)
async def high_throughput_handler(message: Message):
    await fast_async_operation(message.data)


# --8<-- [end:high_concurrency]


# Simulated HTTP client
class HTTPClient:
    async def post(self, url: str, json: dict) -> None:
        pass


http_client = HTTPClient()


# --8<-- [start:io_bound]
# High concurrency for I/O-bound tasks (API calls, database queries)
@broker.subscriber(
    alias="api-caller",
    topic_name="api-requests",
    subscription_name="api-requests-subscription",
    max_messages=500,  # High - most time is spent waiting
)
async def call_external_api(message: Message):
    await http_client.post("/api/endpoint", json={"data": message.data})


# --8<-- [end:io_bound]


def compute_heavy_operation(data: bytes) -> dict:
    """Heavy CPU operation."""
    return {}


async def save_result(result: dict) -> None:
    """Save result."""
    pass


# --8<-- [start:cpu_bound]
# Low concurrency for CPU-bound tasks
@broker.subscriber(
    alias="data-processor",
    topic_name="processing-jobs",
    subscription_name="processing-subscription",
    max_messages=10,  # Low - use multiple workers instead
)
async def process_data(message: Message):
    result = compute_heavy_operation(message.data)
    await save_result(result)


# --8<-- [end:cpu_bound]


# Simulated rate-limited client
class RateLimitedClient:
    async def call(self, data: bytes) -> None:
        pass


rate_limited_client = RateLimitedClient()


# --8<-- [start:rate_limited]
# Match the API rate limit
@broker.subscriber(
    alias="rate-limited-api",
    topic_name="rate-limited-requests",
    subscription_name="rate-limited-subscription",
    max_messages=50,  # Match API's rate limit
)
async def call_rate_limited_api(message: Message):
    await rate_limited_client.call(message.data)


# --8<-- [end:rate_limited]


# --8<-- [start:ack_deadline]
@broker.subscriber(
    alias="slow-processor",
    topic_name="heavy-tasks",
    subscription_name="heavy-tasks-subscription",
    ack_deadline_seconds=600,
    max_messages=10,
)
async def slow_handler(message: Message):
    await complex_ml_inference(message.data)


# --8<-- [end:ack_deadline]


# --8<-- [start:transient_backoff]
# Short backoff for transient issues (network blips)
@broker.subscriber(
    alias="network-sensitive",
    topic_name="transient-events",
    subscription_name="transient-events-subscription",
    min_backoff_delay_secs=5,
    max_backoff_delay_secs=60,
    max_delivery_attempts=5,
)
async def handle_transient_event(message: Message):
    await send_notification(message.data)


# --8<-- [end:transient_backoff]


# Simulated external service
class ExternalService:
    async def process(self, data: bytes) -> None:
        pass


external_service = ExternalService()


# --8<-- [start:external_backoff]
# Longer backoff for external service outages
@broker.subscriber(
    alias="external-api",
    topic_name="external-api-calls",
    subscription_name="external-api-subscription",
    min_backoff_delay_secs=30,
    max_backoff_delay_secs=600,
    max_delivery_attempts=10,
)
async def call_external_service(message: Message):
    await external_service.process(message.data)


# --8<-- [end:external_backoff]


# --8<-- [start:retry_backoff]
@broker.subscriber(
    alias="api-with-backoff",
    topic_name="api-calls",
    subscription_name="api-calls-subscription",
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=600,
    max_delivery_attempts=10,
    dead_letter_topic="api-calls-dlq",
)
async def call_api_with_backoff(message: Message):
    await external_service.process(message.data)


# --8<-- [end:retry_backoff]


# --8<-- [start:complete_tuned]
tuned_broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    shutdown_timeout=30.0,
    middlewares=[Middleware(GZipMiddleware, compresslevel=6)],
)
tuned_app = FastPubSub(tuned_broker)


@tuned_broker.subscriber(
    alias="optimized-processor",
    topic_name="optimized-events",
    subscription_name="optimized-events-subscription",
    # Concurrency
    max_messages=200,
    # Timeouts
    ack_deadline_seconds=120,
    # Retry policy
    min_backoff_delay_secs=10,
    max_backoff_delay_secs=300,
    max_delivery_attempts=5,
    # Error handling
    dead_letter_topic="events-dlq",
    autocreate=True,
)
async def process_optimized_event(message: Message):
    await handle_event(message.data)


# --8<-- [end:complete_tuned]
