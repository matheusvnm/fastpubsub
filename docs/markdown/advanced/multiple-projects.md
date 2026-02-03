---
icon: lucide/folder-tree
---

# Cross-Project Configuration

FastPubSub can subscribe to topics and publish messages across different Google Cloud projects. This enables architectures where services in one project consume events from another project, or where a shared event bus spans multiple projects.

## Why Use Cross-Project Messaging?

Cross-project messaging is useful when:

- **Microservices across projects** - Different teams own different projects but need to communicate
- **Shared event bus** - A central project hosts common events consumed by multiple projects
- **Data pipelines** - Data flows from source projects to processing projects
- **Multi-environment setups** - Production services consume events from staging for testing

!!! info "IAM Permissions Required"
    Cross-project access requires proper IAM permissions. The service account running your application must have Pub/Sub permissions in the target project.

## Cross-Project Subscribers

Override the broker's default project on individual subscribers using the `project_id` parameter:

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

# Main broker uses project-a
broker = PubSubBroker(project_id="project-a")  # (1)!
app = FastPubSub(broker)

# This subscriber uses the default project (project-a)
@broker.subscriber(
    alias="local-handler",
    topic_name="local-events",
    subscription_name="local-events-subscription",
)
async def handle_local_events(message: Message):
    await process_local_event(message.data)

# This subscriber uses a different project (project-b)
@broker.subscriber(
    alias="cross-project-handler",
    topic_name="shared-events",
    subscription_name="project-a-subscription",
    project_id="project-b",  # (2)!
    autocreate=True,
)
async def handle_cross_project_events(message: Message):
    await process_shared_event(message.data)
```

1. Default project for all subscribers
2. Override for this specific subscriber

!!! tip "Subscription Naming"
    When subscribing across projects, name your subscription to indicate which project owns it (e.g., `project-a-subscription`). This helps identify which service is consuming events.

## Cross-Project Publishers

Create publishers that send messages to topics in different projects:

```python
broker = PubSubBroker(project_id="project-a")
app = FastPubSub(broker)

# Publisher for the default project
local_publisher = broker.publisher("local-events")

# Publisher for a different project
cross_project_publisher = broker.publisher(
    "shared-events",
    project_id="project-b"  # (1)!
)

@app.post("/send-event")
async def send_event(data: dict):
    # Publish to local project
    await local_publisher.publish(data)

    # Publish to other project
    await cross_project_publisher.publish(data)
```

1. Target topic is in project-b

You can also publish directly with the broker:

```python
# Publish to default project
await broker.publish("local-events", {"event": "local"})

# Publish to specific project
await broker.publish(
    "shared-events",
    {"event": "cross-project"},
    project_id="project-b"
)
```

## Router-Level Cross-Project

Use routers to organize subscribers by project. All subscribers in the router inherit the router's project:

```python
from fastpubsub import PubSubBroker, PubSubRouter, FastPubSub, Message

broker = PubSubBroker(project_id="project-a")
app = FastPubSub(broker)

# Router for external project
external_router = PubSubRouter(
    prefix="external",
    project_id="project-b"  # (1)!
)

@external_router.subscriber(
    alias="shared-handler",  # (2)!
    topic_name="shared-events",
    subscription_name="project-a-subscription",
)
async def handle_shared(message: Message):
    await process_shared_event(message.data)

@external_router.subscriber(
    alias="analytics-handler",
    topic_name="analytics-events",
    subscription_name="project-a-analytics-subscription",
)
async def handle_analytics(message: Message):
    await process_analytics(message.data)

# Include the router in the broker
broker.include_router(external_router)
```

1. All subscribers in this router use project-b
2. Full alias becomes "external.shared-handler"

### Nested Routers

Routers can be nested, with each level potentially overriding the project:

```python
broker = PubSubBroker(project_id="project-a")

# First level router - uses project-b
level1_router = PubSubRouter(
    prefix="external",
    project_id="project-b"
)

# Second level router - uses project-c
level2_router = PubSubRouter(
    prefix="analytics",
    project_id="project-c"
)

# Subscriber uses project-c (inherited from level2)
@level2_router.subscriber(
    alias="handler",
    topic_name="metrics",
    subscription_name="metrics-subscription",
)
async def handle_metrics(message: Message):
    pass

level1_router.include_router(level2_router)
broker.include_router(level1_router)
```

## Complete Example

A service that consumes events from multiple projects:

```python
from fastpubsub import FastPubSub, PubSubBroker, PubSubRouter, Message

# Main project
broker = PubSubBroker(project_id="my-service")
app = FastPubSub(broker)

# Local events
@broker.subscriber(
    alias="local-orders",
    topic_name="orders",
    subscription_name="orders-subscription",
)
async def handle_local_orders(message: Message):
    await process_order(message.data)

# Events from shared platform
platform_router = PubSubRouter(
    prefix="platform",
    project_id="shared-platform"
)

@platform_router.subscriber(
    alias="user-events",
    topic_name="user-events",
    subscription_name="my-service-user-subscription",
)
async def handle_user_events(message: Message):
    await sync_user_data(message.data)

@platform_router.subscriber(
    alias="notifications",
    topic_name="notifications",
    subscription_name="my-service-notifications-subscription",
)
async def handle_notifications(message: Message):
    await send_notification(message.data)

broker.include_router(platform_router)

# Publishing to both projects
@app.post("/create-order")
async def create_order(order: dict):
    # Local publish
    await broker.publish("orders", order)

    # Notify platform
    await broker.publish(
        "order-events",
        {"order_id": order["id"], "action": "created"},
        project_id="shared-platform"
    )
```

??? example "See cross-project examples"
    Check out the complete examples:

    - [e1_03_cross_project_subscribers.py](../../snippets/basic_usage/e1_03_cross_project_subscribers.py) - Subscribing across projects
    - [e2_04_cross_project_publisher.py](../../snippets/basic_usage/e2_04_cross_project_publisher.py) - Publishing across projects
    - [e1_02_cross_project_router.py](../../snippets/routers/e1_02_cross_project_router.py) - Router-level project configuration

## IAM Configuration

For cross-project access to work, configure IAM permissions:

### Subscribing to Another Project's Topic

Grant your service account the `roles/pubsub.subscriber` role in the target project:

```bash
# Grant subscription permissions in project-b to service account from project-a
gcloud projects add-iam-policy-binding project-b \
    --member="serviceAccount:my-service@project-a.iam.gserviceaccount.com" \
    --role="roles/pubsub.subscriber"
```

### Publishing to Another Project's Topic

Grant your service account the `roles/pubsub.publisher` role:

```bash
# Grant publish permissions in project-b to service account from project-a
gcloud projects add-iam-policy-binding project-b \
    --member="serviceAccount:my-service@project-a.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"
```

!!! warning "Principle of Least Privilege"
    Grant only the permissions needed. Use topic-level or subscription-level IAM bindings instead of project-level when possible.

## Best Practices

!!! tip "Use Descriptive Subscription Names"
    Include the consuming project in subscription names (e.g., `project-a-orders-subscription`) to easily identify which services are consuming which topics.

!!! tip "Document Cross-Project Dependencies"
    Maintain documentation of which services depend on which cross-project topics. This helps during incident response and migration planning.

!!! tip "Test IAM Permissions"
    Before deploying, verify your service account has the necessary permissions in all target projects using `gcloud pubsub topics list` or similar commands.

!!! tip "Use Service Accounts"
    Always use service accounts for cross-project access, not user credentials. This ensures consistent permissions and better security auditing.

## Recap

- **Cross-project subscribers** use `project_id` parameter to override the broker's default project
- **Cross-project publishers** can target topics in any project with proper permissions
- **Routers** simplify cross-project organization by setting project at the router level
- **IAM permissions** must be configured in the target project for cross-project access
- **Use descriptive naming** to identify which services own which subscriptions
