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
--8<-- "advanced/e1_07_multiple_projects.py:cross_project_subscriber"
```

1. Default project for all subscribers
2. Override for this specific subscriber

!!! tip "Subscription Naming"
    When subscribing across projects, name your subscription to indicate which project owns it (e.g., `project-a-subscription`). This helps identify which service is consuming events.

## Cross-Project Publishers

Create publishers that send messages to topics in different projects:

```python
--8<-- "advanced/e1_07_multiple_projects.py:cross_project_publisher"
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

---

## Step-by-Step

1. Decide which project owns each topic and subscription.
2. Grant IAM permissions across projects.
3. Override `project_id` on subscribers or publishers as needed.
4. Verify access with a test publish/consume.

## Router-Level Cross-Project

Use routers to organize subscribers by project. All subscribers in the router inherit the router's project:

```python
--8<-- "advanced/e1_07_multiple_projects.py:router_cross_project"
```

1. All subscribers in this router use project-b
2. Full alias becomes "external.shared-handler"

### Nested Routers

Routers can be nested, with each level potentially overriding the project:

```python
--8<-- "advanced/e1_07_multiple_projects.py:nested_routers"
```

## Complete Example

A service that consumes events from multiple projects:

```python
--8<-- "advanced/e1_07_multiple_projects.py:complete_example"
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

---

## Common Pitfalls

- Missing IAM permissions in the target project.
- Using ambiguous subscription names across projects.
- Forgetting to override `project_id` on cross-project publishers.

## Recap

- **Cross-project subscribers** use `project_id` parameter to override the broker's default project
- **Cross-project publishers** can target topics in any project with proper permissions
- **Routers** simplify cross-project organization by setting project at the router level
- **IAM permissions** must be configured in the target project for cross-project access
- **Use descriptive naming** to identify which services own which subscriptions
