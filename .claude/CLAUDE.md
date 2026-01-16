# FastPubSub Architecture & Design Document

## 1. Executive Summary

**FastPubSub** is a high-performance, async-first framework for building Google Cloud Pub/Sub message consumers and publishers. It mirrors the developer experience of FastAPI while providing production-ready features for event-driven microservices.

### Target Audience
- Microservices requiring event-driven communication
- Systems needing reliable message processing with retry/dead-letter patterns
- Teams familiar with FastAPI seeking similar DX for message handling

### Key Differentiators
- **FastAPI-inspired API**: Decorators, routers, middleware chains, type safety via Pydantic
- **Async-first design**: Bridges Google's threading-based SDK with Python's asyncio
- **Production-ready**: Built-in health checks, graceful shutdown, structured logging
- **Composable architecture**: Hierarchical routers with prefix namespacing

---

## 2. Design Philosophy & Principles

### 2.1 FastAPI-Inspired Developer Experience

FastPubSub deliberately mirrors FastAPI patterns to minimize learning curve:

| FastAPI Pattern | FastPubSub Equivalent |
|-----------------|----------------------|
| `@app.get("/path")` | `@broker.subscriber(alias, topic_name, ...)` |
| `APIRouter` | `PubSubRouter` |
| `app.include_router()` | `broker.include_router()` |
| Middleware classes | `BaseMiddleware` with `on_message`/`on_publish` |
| Pydantic validation | Same - strict type hints enforced by `@validate_call` |

### 2.2 Async-First Architecture

The framework bridges two concurrency models:

1. **Google Cloud Pub/Sub SDK**: Uses gRPC with threading-based `StreamingPullFuture`
2. **FastPubSub**: Uses Python's `asyncio` event loop for non-blocking handlers

This is achieved through `AsyncScheduler` which schedules callbacks from gRPC threads into the asyncio event loop via `loop.call_soon_threadsafe()`.

### 2.3 Production-Ready by Default

- **Health endpoints**: `/consumers/alive` (liveness), `/consumers/ready` (readiness)
- **Graceful shutdown**: Configurable timeout, waits for in-flight messages
- **Structured logging**: JSON output option for log aggregation platforms
- **Retry patterns**: `Drop` and `Retry` exceptions for explicit control

### 2.4 Composability & Modularity

- **Middleware chains**: Chain of Responsibility pattern for cross-cutting concerns
- **Hierarchical routers**: Domain-driven design with prefix namespacing
- **Separation of concerns**: HTTP (FastAPI) vs Message processing (PubSubBroker)

---

## 3. Core Architecture Deep Dive

### 3.1 Component Hierarchy

```
FastPubSub (FastAPI Application)
├── Application (Lifecycle Manager)
│   ├── on_startup / after_startup hooks
│   └── on_shutdown / after_shutdown hooks
└── PubSubBroker (Message Bus Orchestrator)
    ├── PubSubRouter (Hierarchical Organization)
    │   ├── subscribers: dict[alias, Subscriber]
    │   ├── publishers: WeakSet[Publisher]
    │   ├── routers: list[PubSubRouter]  (nested)
    │   └── middlewares: MutableSequence[Middleware]
    └── AsyncTaskManager (Concurrency Control)
        └── PubSubStreamingPullTask[] (per subscription)
            └── AsyncScheduler (Thread-Event Loop Bridge)
```

### 3.2 Message Flow Architecture

#### Publishing Flow

```
User Code
    │
    ▼
Publisher.publish(data, ordering_key, attributes)
    │
    ├── _serialize_message(data)
    │   ├── bytes → bytes (passthrough)
    │   ├── str → UTF-8 bytes
    │   ├── dict → JSON compact bytes
    │   └── BaseModel → Pydantic JSON bytes
    │
    ├── _build_callstack()  [Middlewares in REVERSE order]
    │   ├── PublishMessageSerializerMiddleware (terminal)
    │   └── User Middlewares... (reversed)
    │
    └── callstack.on_publish(data, ordering_key, attributes)
        │
        ▼
    Google Cloud Pub/Sub API
```

#### Consuming Flow

```
Google Cloud Pub/Sub (gRPC thread)
    │
    ▼
StreamingPullFuture delivers message
    │
    ▼
AsyncScheduler.schedule(callback, message)
    │
    └── loop.call_soon_threadsafe(callback)
        │
        ▼
Event Loop Thread
    │
    ▼
PubSubStreamingPullTask._on_message(received_message)
    │
    ├── loop.create_task(_consume(received_message))
    └── scheduler.register_task_execution(task, message)
        │
        ▼
_consume(received_message)
    │
    ├── MessageMapper.convert() → fastpubsub.Message
    │
    ├── subscriber._build_callstack()  [Middlewares in REVERSE order]
    │   ├── HandleMessageSerializerMiddleware (terminal → calls handler)
    │   └── User Middlewares... (reversed)
    │
    ├── callstack.on_message(message)
    │
    └── Handle outcome:
        ├── Success → ack_with_response()
        ├── Drop exception → ack_with_response() (silent drop)
        ├── Retry exception → nack_with_response() (retry later)
        └── Other exception → nack_with_response() (log & retry)
```

### 3.3 Concurrency Model

#### AsyncScheduler (`fastpubsub/clients/scheduler.py`)

Bridges gRPC's threading model to asyncio:

```python
class AsyncScheduler(Scheduler):
    def __init__(self, loop: AbstractEventLoop):
        self._loop = loop
        self._pending_task_creations: WeakKeyDictionary[Handle, PubSubMessage]
        self._executing_tasks: dict[int, PubSubMessage]
        self._executing_lock = threading.Lock()  # Thread-safe
        self.closed = False
```

**Key design decisions:**
- `WeakKeyDictionary` for pending handles - auto-cleanup when handle GC'd
- `threading.Lock` for `_executing_tasks` - accessed from both gRPC and event loop threads
- `closed` flag prevents new messages during shutdown

#### AsyncTaskManager (`fastpubsub/concurrency/manager.py`)

Orchestrates all subscription tasks:

```python
class AsyncTaskManager:
    def __init__(self):
        self._tasks: list[PubSubStreamingPullTask] = []

    async def shutdown(self, timeout: float = 30.0):
        # Uses asyncio.TaskGroup for concurrent shutdown of all tasks
        async with asyncio.timeout(delay=timeout):
            async with asyncio.TaskGroup() as tg:
                for task in self._tasks:
                    if task.task_alive():
                        tg.create_task(task.shutdown(timeout=timeout))
```

---

## 4. Key Design Patterns

### 4.1 Chain of Responsibility (Middleware)

Middlewares form a linked-list chain with `next_call` pointer:

```python
class BaseMiddleware:
    def __init__(self, next_call: BaseMiddleware | None):
        self.next_call = next_call

    async def on_message(self, message: Message) -> Any:
        # Do pre-processing
        if not self.next_call:
            return
        result = await self.next_call.on_message(message)
        # Do post-processing
        return result
```

**Middleware application order:**

```python
# User adds: [LoggingMiddleware, GZipMiddleware]
# Chain built in REVERSE:
# GZip → Logging → Terminal(handler)
#
# Execution order for on_message:
# 1. GZip.on_message (decompress)
# 2. Logging.on_message (log)
# 3. Terminal.on_message (invoke handler)
```

**Three levels of middleware application:**
1. **Broker level**: Applied to ALL subscribers/publishers
2. **Router level**: Applied to router's subscribers/publishers
3. **Subscriber/Publisher level**: Applied to single entity

### 4.2 Decorator/Builder Pattern

Registration via decorators mirrors FastAPI:

```python
@broker.subscriber(
    alias="user-events",
    topic_name="users",
    subscription_name="user-events-sub",
    max_messages=1000,
    dead_letter_topic="users-dlq",
)
async def handle_user_event(msg: Message) -> None:
    pass
```

The decorator:
1. Validates function is async (`ensure_async_callable_function`)
2. Creates `Subscriber` instance with policies
3. Registers in router's `subscribers` dict
4. Returns original function unchanged

### 4.3 Strategy Pattern (Policies)

Immutable policy dataclasses encapsulate configuration:

```python
@dataclass(frozen=True)
class MessageRetryPolicy:
    min_backoff_delay_secs: int
    max_backoff_delay_secs: int

@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    max_delivery_attempts: int

@dataclass(frozen=True)
class LifecyclePolicy:
    autocreate: bool
    autoupdate: bool
```

**Why frozen dataclasses?**
- **Immutability**: Prevents accidental configuration mutation
- **Thread safety**: No synchronization needed
- **Hashability**: Can be used as dict keys if needed

### 4.4 Factory Pattern (PubSubClientFactory)

Singleton clients per configuration to reuse gRPC connections:

```python
class PubSubClientFactory:
    _publisher_cache: dict[(str, bool), PublisherClient] = {}
    _subscriber_cache: dict[str, SubscriberClient] = {}

    @classmethod
    async def get_publisher(cls, project_id, enable_ordering=False):
        key = (project_id, enable_ordering)
        if key not in cls._publisher_cache:
            async with cls._get_lock():
                if key not in cls._publisher_cache:  # Double-checked locking
                    cls._publisher_cache[key] = PublisherClient(...)
        return cls._publisher_cache[key]
```

### 4.5 Weak Reference Pattern (Publishers)

Publishers use `WeakSet` for memory management:

```python
class PubSubRouter:
    publishers: WeakSet[Publisher] = WeakSet()
```

**Why WeakSet?**
- Publishers are transient (created on-demand via `broker.publisher()`)
- Each call creates a NEW instance (not cached)
- WeakSet auto-removes when publisher is garbage collected
- Prevents memory leaks from accumulating temporary publishers

---

## 5. Router Hierarchy & Prefix Management

### 5.1 Nesting Model

Routers can be nested arbitrarily deep:

```python
level3 = PubSubRouter(prefix="level3")
level2 = PubSubRouter(prefix="level2", routers=(level3,))
level1 = PubSubRouter(prefix="level1", routers=(level2,))

broker.include_router(level1)
# Resulting prefixes:
# level1.prefix = "level1"
# level2.prefix = "level1.level2"
# level3.prefix = "level1.level2.level3"
```

### 5.2 Prefix Validation

```python
_PREFIX_REGEX = re.compile(r"^[a-zA-Z0-9]+([_./][a-zA-Z0-9]+)*$")
# Valid: "a", "a.b", "a_b", "a/b", "a.b_c/d"
# Invalid: ".a", "_a", "/a", "a.", "a_", "a/", "a..b"
```

### 5.3 Configuration Propagation

When a router is included, configuration flows downward:

```python
def include_router(self, router: PubSubRouter):
    router._add_prefix(self.prefix)      # Prepend prefix
    router._set_project_id(self.project_id)  # Inherit project_id

    # Propagate ALL middlewares from parent
    for middleware, args, kwargs in self.middlewares:
        router.include_middleware(middleware, *args, **kwargs)
```

### 5.4 Conflict Detection

Alias conflicts are detected at startup:

```python
def _get_subscribers(self) -> dict[str, Subscriber]:
    subscribers = {}
    subscribers.update(self.subscribers)

    for router in self.routers:
        for alias, subscriber in router._get_subscribers().items():
            if alias in subscribers:
                raise FastPubSubException(
                    f"Conflict on subscribers. Alias={alias}"
                )
        subscribers.update(router_subscribers)

    return subscribers
```

---

## 6. Message Acknowledgment Patterns

### 6.1 Exception-Based Control Flow

```python
from fastpubsub.exceptions import Drop, Retry

@broker.subscriber(...)
async def handler(msg: Message):
    if is_duplicate(msg):
        raise Drop()  # ACK - permanently discard

    if external_service_unavailable():
        raise Retry()  # NACK - retry with backoff

    process(msg)  # Success - ACK
```

### 6.2 Acknowledgment Response Handling

```python
async def _wait_acknowledge_response(self, future: Future):
    try:
        await apply_async(future.result)
    except AcknowledgeError as e:
        match e.error_code:
            case AcknowledgeStatus.PERMISSION_DENIED:
                logger.exception("No permission to ack/nack")
            case AcknowledgeStatus.FAILED_PRECONDITION:
                logger.exception("Subscription detached or key access issue")
            case AcknowledgeStatus.INVALID_ACK_ID:
                logger.info("Ack ID expired, message will be redelivered")
```

---

## 7. Testing Utilities

### 7.1 PubSubTestClient (`fastpubsub/testing.py`)

In-memory testing without emulator or credentials:

```python
from fastpubsub.testing import PubSubTestClient

async def test_handler():
    broker = PubSubBroker(project_id="test")

    @broker.subscriber(alias="test", topic_name="topic", subscription_name="sub")
    async def handler(msg: Message):
        assert msg.data == b'{"key": "value"}'

    async with PubSubTestClient(broker) as client:
        await client.publish({"key": "value"}, topic="topic")

        # Inspect published messages
        messages = client.get_published_messages()
        assert len(messages) == 1
```

### 7.2 Patching Strategy

PubSubTestClient patches:

1. `fastpubsub.clients.pubsub.PubSubClient` - prevents real API calls
2. `fastpubsub.builder.PubSubSubscriptionBuilder` - skips subscription creation
3. `broker.task_manager.start/shutdown` - disables background tasks

### 7.3 Message Routing

Published messages are routed to matching subscribers synchronously:

```python
async def _fake_publish(self, topic_name, data, ordering_key, attributes):
    self._published_messages.append((topic_name, data, attributes))

    subscribers = self.broker.router._get_subscribers()
    for subscriber in subscribers.values():
        if subscriber.topic_name == topic_name:
            message = Message(...)
            callstack = subscriber._build_callstack()
            await callstack.on_message(message)  # Synchronous!
```

---

## 8. Graceful Shutdown

### 8.1 Multi-Phase Shutdown

**Phase 1: Stop accepting new messages**
```python
# Cancel StreamingPullFuture
self.task.cancel()
```

**Phase 2: Wait for in-flight messages**
```python
async def wait_for_completion(self, timeout: float = 30.0) -> bool:
    self.closed = True  # Reject new messages
    start_time = self._loop.time()

    while self._loop.time() - start_time < timeout:
        pending = len(self._pending_task_creations)
        executing = len(self._executing_tasks)

        if pending == 0 and executing == 0:
            return True  # All done

        await asyncio.sleep(0.5)

    return False  # Timeout
```

**Phase 3: Cleanup resources**
```python
await PubSubClientFactory.close_all()  # Close gRPC connections
```

### 8.2 Kubernetes Integration

```yaml
spec:
  containers:
    - name: consumer
      livenessProbe:
        httpGet:
          path: /consumers/alive
          port: 8000
      readinessProbe:
        httpGet:
          path: /consumers/ready
          port: 8000
  terminationGracePeriodSeconds: 60  # Match shutdown_timeout
```

---

## 9. Configuration & Policies

### 9.1 Subscriber Configuration

```python
@broker.subscriber(
    # Identity
    alias="user-events",                    # Unique handler name
    topic_name="users",                     # Source topic
    subscription_name="user-events-sub",    # Subscription name
    project_id="alt-project",               # Override broker's project_id

    # Lifecycle
    autocreate=True,                        # Auto-create topic/subscription
    autoupdate=False,                       # Auto-update subscription config

    # Delivery
    filter_expression="attributes.type='user'",  # Server-side filtering
    ack_deadline_seconds=60,                # Ack deadline
    enable_message_ordering=False,          # Ordered delivery
    enable_exactly_once_delivery=False,     # Exactly-once semantics

    # Retry
    min_backoff_delay_secs=10,              # Min exponential backoff
    max_backoff_delay_secs=600,             # Max exponential backoff

    # Dead-letter
    dead_letter_topic="users-dlq",          # DLQ topic
    max_delivery_attempts=5,                # Attempts before DLQ

    # Flow control
    max_messages=1000,                      # Concurrent message limit

    # Middlewares (subscriber-specific)
    middlewares=[Middleware(CustomMiddleware)],
)
async def handler(msg: Message):
    pass
```

### 9.2 Environment Variables

```bash
# Subscriber selection
FASTPUBSUB_SUBSCRIBERS=alias1,alias2    # Comma-separated aliases

# Logging
FASTPUBSUB_LOG_LEVEL=DEBUG              # Log level
FASTPUBSUB_ENABLE_LOG_COLORS=1          # Colorized output
FASTPUBSUB_ENABLE_LOG_SERIALIZE=1       # JSON format
```

---

## 10. Sync/Async Bridging

### 10.1 The Problem

Google Cloud Pub/Sub SDK uses gRPC with threading-based callbacks. FastPubSub uses asyncio.

### 10.2 The Solution

**AsyncScheduler bridges the gap:**

```python
# gRPC Thread (blocking)
def schedule(self, callback, *args):
    # This runs in gRPC thread
    wrapped = functools.partial(callback, message)

    # Schedule into event loop (thread-safe)
    handle = self._loop.call_soon_threadsafe(wrapped)

    # Track pending (WeakKeyDictionary - auto cleanup)
    self._pending_task_creations[handle] = message

    return handle

# Event Loop Thread (async)
def _on_message(received_message):
    # This runs in event loop
    coro = self._consume(received_message)
    task = self.loop.create_task(coro)
    self.scheduler.register_task_execution(task, received_message)
    return task
```

### 10.3 Blocking-to-Async Utilities

```python
# fastpubsub/concurrency/utils.py
async def apply_async(func, *args, **kwargs) -> T:
    """Run blocking function in thread pool."""
    partial_func = functools.partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(partial_func, abandon_on_cancel=False)

# Usage: Waiting for Google's blocking Future.result()
response: Future[str] = publisher_client.publish(...)
message_id = await apply_async(response.result)
```

---

## 11. Logging & Observability

### 11.1 Context-Aware Logging

```python
# In message handler
with logger.contextualize(
    message_id=message.id,
    topic_name=message.topic_name,
    subscriber_name=message.subscriber_name,
):
    logger.info("Processing message")
    # Output includes: message_id=abc topic_name=users subscriber_name=...
```

### 11.2 Output Formats

**Human-readable (default):**
```
2026-01-16 10:30:45 | INFO | 12345:67890 | tasks:_consume:123 | Processing message | message_id=abc
```

**JSON (with `FASTPUBSUB_ENABLE_LOG_SERIALIZE=1`):**
```json
{"timestamp": "2026-01-16T10:30:45", "level": "INFO", "message": "Processing message", "context": {"message_id": "abc"}}
```

---

## 12. Architecture Decision Records (ADRs)

### ADR-1: Why WeakSet for Publishers?

**Context**: Publishers are created on-demand via `broker.publisher(topic)`.

**Decision**: Use `WeakSet` instead of regular `set` or `list`.

**Rationale**:
- Each call creates a NEW Publisher instance (not cached)
- Prevents memory leaks from accumulating temporary publishers
- Auto-cleanup when publisher goes out of scope

### ADR-2: Why Reverse Middleware Order?

**Context**: Users add middlewares in intuitive order: `[First, Second, Third]`.

**Decision**: Reverse the list when building the callstack.

**Rationale**:
- First-added middleware should be outermost (executed first)
- Building chain from terminal outward requires reverse iteration
- Result: `Third → Second → First → Terminal`

### ADR-3: Why Custom AsyncScheduler?

**Context**: Google's StreamingPullFuture uses threading-based callbacks.

**Decision**: Implement custom `Scheduler` subclass with asyncio integration.

**Rationale**:
- Cannot modify Google's SDK behavior
- `call_soon_threadsafe` bridges threads to event loop
- Enables tracking pending/executing tasks for graceful shutdown

### ADR-4: Why Frozen Policy Dataclasses?

**Context**: Configuration objects are created once and used throughout.

**Decision**: Use `@dataclass(frozen=True)` for all policy classes.

**Rationale**:
- Immutability prevents accidental mutation
- Thread-safe without synchronization
- Clear intent: these are configuration, not mutable state

### ADR-5: Why Separate Message Dataclass?

**Context**: Google's `PubSubMessage` is mutable and has complex API.

**Decision**: Create immutable `fastpubsub.Message` dataclass.

**Rationale**:
- Decouples from Google's implementation
- Simpler interface for user handlers
- Immutable, thread-safe
- Contains only data needed by handlers

---

## 13. File Structure Reference

```
fastpubsub/
├── __init__.py              # Public API exports
├── applications.py          # FastPubSub app + Application lifecycle
├── broker.py               # PubSubBroker orchestrator
├── router.py               # PubSubRouter hierarchy + prefix management
├── builder.py              # PubSubSubscriptionBuilder (topic/sub creation)
├── pubsub/
│   ├── __init__.py
│   ├── publisher.py        # Publisher class + message serialization
│   └── subscriber.py       # Subscriber class + middleware building
├── middlewares/
│   ├── __init__.py
│   ├── base.py            # BaseMiddleware + Middleware wrapper
│   ├── di.py              # Terminal middlewares (Handle/Publish serializers)
│   └── gzip.py            # GZip compression middleware
├── concurrency/
│   ├── __init__.py
│   ├── utils.py           # apply_async, ensure_async_callable_function
│   ├── manager.py         # AsyncTaskManager orchestrator
│   └── tasks.py           # PubSubStreamingPullTask per subscription
├── clients/
│   ├── __init__.py
│   ├── pubsub.py          # PubSubClient (Google Cloud wrapper)
│   ├── factory.py         # PubSubClientFactory singleton cache
│   └── scheduler.py       # AsyncScheduler (thread-event loop bridge)
├── datastructures.py       # Message, Policy dataclasses
├── exceptions.py          # FastPubSubException, Drop, Retry
├── testing.py             # PubSubTestClient
├── logger.py              # FastPubSubLogger with contextualize
├── types.py               # Type aliases
└── cli/                   # Command-line interface
    ├── __init__.py
    ├── main.py
    └── ...
```

---

## 14. Critical Files for Modification

When working on FastPubSub, understand these files first:

| Area | Files | Purpose |
|------|-------|---------|
| **Core Logic** | `applications.py`, `broker.py`, `router.py` | App lifecycle, broker orchestration, routing |
| **Message Handling** | `pubsub/subscriber.py`, `pubsub/publisher.py` | Message consumption and publishing |
| **Concurrency** | `concurrency/tasks.py`, `clients/scheduler.py` | Async task management, thread bridging |
| **Middleware** | `middlewares/base.py`, `middlewares/di.py` | Chain of responsibility, terminal handlers |
| **Configuration** | `datastructures.py`, `exceptions.py` | Policies, Message type, control flow |
| **Testing** | `testing.py` | In-memory test client |

---

## 15. Common Patterns & Examples

### 15.1 Basic Application

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="my-project")
app = FastPubSub(broker)

@broker.subscriber(
    alias="orders",
    topic_name="orders",
    subscription_name="orders-processor",
)
async def process_order(msg: Message) -> None:
    order = json.loads(msg.data)
    # Process order...
```

### 15.2 Modular Router Structure

```python
# users/router.py
from fastpubsub import PubSubRouter, Message

users_router = PubSubRouter(prefix="users")

@users_router.subscriber(
    alias="created",
    topic_name="user-events",
    subscription_name="user-created-handler",
    filter_expression="attributes.event_type='created'",
)
async def handle_user_created(msg: Message) -> None:
    pass

# main.py
from fastpubsub import FastPubSub, PubSubBroker
from users.router import users_router

broker = PubSubBroker(project_id="my-project")
broker.include_router(users_router)

app = FastPubSub(broker)
# Subscriber alias becomes: "users.created"
```

### 15.3 Custom Middleware

```python
from fastpubsub import BaseMiddleware, Middleware, Message
import time

class TimingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        start = time.perf_counter()
        try:
            return await super().on_message(message)
        finally:
            elapsed = time.perf_counter() - start
            logger.info(f"Handler took {elapsed:.3f}s")

# Apply to broker (all subscribers)
broker = PubSubBroker(
    project_id="my-project",
    middlewares=[Middleware(TimingMiddleware)],
)

# Or apply to single subscriber
@broker.subscriber(
    ...,
    middlewares=[Middleware(TimingMiddleware)],
)
async def handler(msg: Message):
    pass
```

### 15.4 Event Chaining

```python
@broker.subscriber(
    alias="stage-1",
    topic_name="raw-events",
    subscription_name="stage-1-processor",
)
async def stage_1(msg: Message) -> None:
    data = json.loads(msg.data)
    enriched = enrich(data)

    # Publish to next stage
    await broker.publish(
        topic_name="enriched-events",
        data=enriched,
        ordering_key=data["id"],
    )

@broker.subscriber(
    alias="stage-2",
    topic_name="enriched-events",
    subscription_name="stage-2-processor",
)
async def stage_2(msg: Message) -> None:
    data = json.loads(msg.data)
    store(data)
```

### 15.5 Error Handling with Drop/Retry

```python
from fastpubsub.exceptions import Drop, Retry

@broker.subscriber(...)
async def handler(msg: Message) -> None:
    data = json.loads(msg.data)

    # Idempotency check
    if await is_already_processed(data["id"]):
        raise Drop()  # Don't process duplicates

    try:
        result = await external_api.call(data)
    except RateLimitError:
        raise Retry()  # Exponential backoff and retry
    except ValidationError:
        raise Drop()  # Invalid data, don't retry

    await save(result)
```

### 15.6 Multi-Project Publishing

```python
broker = PubSubBroker(project_id="main-project")

# Publish to different project
await broker.publish(
    topic_name="cross-project-topic",
    data={"event": "created"},
    project_id="other-project",  # Override main-project
)

# Subscribe from different project
@broker.subscriber(
    alias="cross-project",
    topic_name="external-events",
    subscription_name="external-handler",
    project_id="other-project",  # Override main-project
)
async def handle_external(msg: Message):
    pass
```
