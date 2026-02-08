---
icon: lucide/git-branch
---

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

The key to understanding routers is the aliasing mechanism. When you create a router, you give it a string `prefix` that will be prepended to the `alias` of every subscriber defined within it. The final alias is then used by the CLI as `prefix.alias` to define which subscriber should be started.

**Example:**

- Router: `users_router = PubSubRouter(prefix="users")`
- Subscriber: `@users_router.subscriber(alias="new-user", subscription_name="new-accounts", ...)`
- Final alias: `users.new-user`
- Subscription name: `new-accounts`

---

## How to Use Routers

Using routers is a three-step process:

1. **Create a Router:** Instantiate **PubSubRouter** in a separate module, providing a unique `prefix`.
2. **Define Subscribers:** Use the router's `@router.subscriber()` decorator, just as you would with a broker.
3. **Include the Router:** In your main application file, import the router instance and include it in your central broker using `broker.include_router()` or via constructor.

---

## Step-by-Step

1. Create a router with a clear prefix.
2. Define subscribers on the router.
3. Include the router in the broker.
4. Run specific subscribers with `-s prefix.alias`.


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
--8<-- "routers/multi_domain_routers/user_domain/routers.py"
```

**File: `my_app/posts_domain/router.py`**

```python
--8<-- "routers/multi_domain_routers/posts_domain/routers.py"
```

**File: `my_app/main.py`**

```python
--8<-- "routers/multi_domain_routers/main.py:main_app"

--8<-- "routers/multi_domain_routers/main.py:router_publish"
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

For large applications, nest routers to create a hierarchical structures. When you nest a router, prefixes are **stacked** (concatenated with a dot).

### Example: Financial Application

```python
--8<-- "routers/e1_05_nested_routers_financial.py"
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



## Interface: PubSubBroker vs PubSubRouter

Routers have a broker-like interface. Most things you can do with a broker, can also be done with a router.

### Publishing from Routers

You can publish messages directly from them:

```python
--8<-- "routers/multi_domain_routers/main.py:router_publish"
```


### Cross-Project Routers

Configure a router to use a different GCP project:

```python
--8<-- "routers/e1_07_cross_project_router_simple.py:cross_project_router"
```



### Router Middlewares

Apply middlewares to all subscribers in a router:

```python
--8<-- "routers/e1_06_router_middlewares.py:router_with_middleware"
```

See the [Middlewares](middlewares.md) guide for more details on middleware hierarchy.

---

## Recap

- **Purpose**: Routers organize large applications into domain-specific modules.
- **Prefix is key**: Each router has a prefix that namespaces subscriber aliases.
- **Modular structure**: Define routers in separate files, then use `broker.include_router()` to plug them in.
- **CLI integration**: Select subscribers by fully-qualified alias: `prefix.alias`.
- **Nested routers**: Stack prefixes for deep hierarchies (e.g., `core.banking.handler`).
- **Broker-like interface**: Routers can define subscribers and publish messages.
