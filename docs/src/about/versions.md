---
icon: lucide/git-branch
---

# FastPubSub Versions

FastPubSub enables developers to push systems to production quickly with embedded features. The project also moves fast—new features are added frequently, bugs are fixed regularly, and the code continuously improves.

The current versions are `0.x.x`, reflecting that each version could potentially have breaking changes. Like FastAPI, FastPubSub follows [Semantic Versioning](https://semver.org/) conventions.

You can create production applications with the latest version, but pay attention to the guidelines below.

---

## Pin Your Version

Pin the version of FastPubSub you're using to a specific version that works correctly for your application.

```
# Static pin: Only this exact version
fastpubsub==0.4.0

# Range pin: 0.4.0 or above, but less than 0.5.0
fastpubsub>=0.4.0,<0.5.0
```

The range pin avoids breaking changes but gets patches and fixes (e.g., 0.4.1, 0.4.2).

!!! warning "Pre-1.0 Versions"

    Following Semantic Versioning, any version below `1.0.0` could add breaking changes. While on `0.x.x`, minor changes can add/change features that break public contracts. Pin your application close to the version you're currently using.

If you use tools like `uv`, `poetry`, or `pipenv`, they all have ways to define specific version constraints.

---

## Upgrading: Always Test

Having a suite of automated unit and integration tests is essential when upgrading library versions.

With tests, you can:

1. Safely upgrade `fastpubsub` to a more recent version
2. Run your tests to ensure everything works correctly
3. Pin the dependency to the new version if tests pass

---

## Documentation Code Conventions

To maintain consistency across all documentation and examples, we follow these conventions:

### Standard Placeholders

| Usage | Placeholder |
|-------|-------------|
| Project ID (production) | `"your-project-id"` |
| Project ID (emulator) | `"fastpubsub-local"` |
| Topic names | Hyphen-case: `"user-events"`, `"order-created"` |
| Subscription names | `"{topic-name}-subscription"` |

### Standard Variable Names

| Variable | Name |
|----------|------|
| Broker | `broker` |
| App | `app` |
| Message | `message` |
| Publisher | `{topic}_publisher` or `publisher` |

### Example

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="user-handler",
    topic_name="user-events",
    subscription_name="user-events-subscription",
)
async def handle_message(message: Message):
    print(f"Processing message: {message.id}")
```

These conventions ensure code examples are clear, consistent, and easy to understand.

---

## Changelog

For detailed changes in each version, see the [GitHub Releases](https://github.com/matheusvnm/fastpubsub/releases) page.
