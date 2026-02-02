# Routers

As your application grows, managing all subscribers and publishers in a single file becomes cumbersome. Routers, inspired by FastAPI's `APIRouter`, help you organize and scale your application by dividing it into smaller, modular components.

## Why Use Routers?

* **Logical Separation:** Organize your subscribers and publishers by domain or feature (e.g., `users`, `orders`, `notifications`). This makes your codebase easier to navigate and understand.
* **Code Modularity:** Split your application into multiple Python files and directories. Each module can define its own self-contained router, which is then included in the main application.
* **Avoid Naming Conflicts:** Routers use a `prefix` to namespace the aliases of their subscribers. This prevents two subscribers from different domains from having the same alias (e.g., `users.create` and `orders.create`).
* **Scoped Configurations:** Apply configurations, such as a specific set of middlewares, to an entire group of subscribers at once.

---

## Core Concepts

### Prefix and Aliasing

The key to understanding routers is the aliasing mechanism. When you create a router, you give it a string prefix that will be prepended to the `alias` of every subscriber defined within it. The final alias is then used by the CLI as `prefix.alias` to define which subscriber should be started.

**Example:**

- Router: `users_router = PubSubRouter(prefix="users")`
- Subscriber: `@users_router.subscriber(alias="new-user", subscription_name="new-accounts", ...)`
- Final alias: `users.new-user`
- Final subscriber name: `users.new-accounts`

---

## How to Use Routers

Using routers is a three-step process:

1. **Create a Router:** Instantiate **PubSubRouter** in a separate module, providing a unique **prefix**.
2. **Define Subscribers:** Use the router's `@router.subscriber()` decorator, just as you would with a broker.
3. **Include the Router:** In your main application file, import the router instance and include it in your central broker using `broker.include_router()` or via construtor.


### Example: Multi-File Application

**Project structure:**

```
my_app/
├── users_domain/
│   └── router.py
├── posts_domain/
│   └── router.py
└── main.py
```

**File: `my_app/users_domain/router.py`**

```python
from fastpubsub import PubSubRouter, Message
from fastpubsub.logger import logger

# 1. Create a router with 'users' prefix
users_router = PubSubRouter(prefix="users")

@users_router.subscriber(
    alias="created",
    topic_name="users-topic",
    subscription_name="users-subscription",
)
async def handle_user_message(message: Message):
    logger.info(f"Processing message {message.id} in users domain.")
```

**File: `my_app/posts_domain/router.py`**

```python
from fastpubsub import PubSubRouter, Message
from fastpubsub.logger import logger

# 1. Create a router with 'posts' prefix
posts_router = PubSubRouter(prefix="posts")

@posts_router.subscriber(
    alias="published",
    topic_name="posts-topic",
    subscription_name="posts-subscription",
)
async def handle_post_message(message: Message):
    logger.info(f"Processing message {message.id} in posts domain.")
```

**File: `my_app/main.py`**

```python
from fastpubsub import FastPubSub, PubSubBroker

# Import router instances
from my_app.users_domain.router import users_router
from my_app.posts_domain.router import posts_router

broker = PubSubBroker("your-project-id")

# 3. Include the routers
broker.include_router(users_router)
broker.include_router(posts_router)

app = FastPubSub(broker)

@app.after_startup
async def publish_test_messages():
    # Routers can also publish messages
    await users_router.publish("users-topic", data={"username": "Yugi"})
    await posts_router.publish("posts-topic", data={"title": "My New Post"})
```

### Running Specific Subscribers

Use fully-qualified aliases (`prefix.alias`) with the `-s` flag:

```bash
# Set up your environment
export PUBSUB_EMULATOR_HOST=localhost:8085

# Run only the users subscriber
fastpubsub run my_app.main:app -s users.created

# Run subscribers from both routers
fastpubsub run my_app.main:app -s users.created -s posts.published
```

---

## Nested Routers

For large applications, nest routers to create a hierarchical structure. When you nest a router, prefixes are **stacked** (concatenated with a dot).

### Example: Financial Application

```python
from fastpubsub import FastPubSub, PubSubBroker, PubSubRouter, Message
from fastpubsub.logger import logger

# Level 2: Sub-domain routers
banking_router = PubSubRouter(prefix="banking")
finance_router = PubSubRouter(prefix="finance")

# Level 1: Main domain router
core_router = PubSubRouter(prefix="core")
core_router.include_router(banking_router)
core_router.include_router(finance_router)


@core_router.subscriber(
    alias="core_handler",
    topic_name="core-topic",
    subscription_name="core-subscription",
)
async def handle_message_core(message: Message):
    logger.info(f"CORE handler received message {message.id}")

@banking_router.subscriber(
    alias="banking_handler",
    topic_name="banking-topic",
    subscription_name="banking-subscription",
)
async def handle_message_banking(message: Message):
    logger.info(f"BANKING handler received message {message.id}")

@finance_router.subscriber(
    alias="finance_handler",
    topic_name="finance-topic",
    subscription_name="finance-subscription",
)
async def handle_message_finance(message: Message):
    logger.info(f"FINANCE handler received message {message.id}")


# Application setup
broker = PubSubBroker(project_id="your-project-id")
broker.include_router(core_router)

app = FastPubSub(broker)
```

### Resulting Aliases

| Handler | Stacked Prefixes | Subscriber Alias | Final Alias |
|---------|------------------|------------------|-------------|
| `handle_message_core` | `core` | `core_handler` | `core.core_handler` |
| `handle_message_banking` | `core` + `banking` | `banking_handler` | `core.banking.banking_handler` |
| `handle_message_finance` | `core` + `finance` | `finance_handler` | `core.finance.finance_handler` |

### CLI Commands

```bash
# Run only the finance handler
fastpubsub run main:app -s core.finance.finance_handler

# Run both core and banking handlers
fastpubsub run main:app -s core.core_handler -s core.banking.banking_handler
```

---

## Publishing from Routers

Routers have a broker-like interface. You can publish messages directly from them:

```python
# Publish from a router
await users_router.publish("users-topic", data={"event": "user_created"})

# This is equivalent to publishing from the broker
await broker.publish("users-topic", data={"event": "user_created"})
```

---

## Cross-Project Routers

Configure a router to use a different GCP project:

```python
# All subscribers in this router use project-b
external_router = PubSubRouter(
    prefix="external",
    project_id="project-b"
)

@external_router.subscriber(
    alias="handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handle_external_event(message: Message):
    pass

broker.include_router(external_router)
```

---

## Router Middlewares

Apply middlewares to all subscribers in a router:

```python
from fastpubsub import PubSubRouter, Middleware
from my_app.middlewares import DomainLoggingMiddleware

# Middleware applies to all subscribers in this router
users_router = PubSubRouter(
    prefix="users",
    middlewares=[Middleware(DomainLoggingMiddleware)]
)
```

See the [Middlewares](middlewares.md) guide for more details on middleware hierarchy.

---

## Recap

- **Purpose**: Routers organize large applications into domain-specific modules
- **Prefix is key**: Each router has a prefix that namespaces subscriber aliases
- **Modular structure**: Define routers in separate files, then use `broker.include_router()` to plug them in
- **CLI integration**: Select subscribers by fully-qualified alias: `prefix.alias`
- **Nested routers**: Stack prefixes for deep hierarchies (e.g., `core.banking.handler`)
- **Broker-like interface**: Routers can define subscribers and publish messages
