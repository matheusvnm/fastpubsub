---
icon: lucide/search
---

# Inspecting Your Application

As your application grows — more subscribers, more routers, more topics — it becomes hard to remember how everything is wired together. The `inspect` command gives you a quick overview of your application's subscribers without starting the server or connecting to Google Cloud.

## How It Works

When you run `fastpubsub inspect subscribers`, the CLI:

1. **Imports your application** using the `module:attribute` path you provide
2. **Reads the subscriber registry** from your broker's router hierarchy
3. **Formats the output** as a table (default) or JSON
4. **Exits** — no server starts, no lifecycle hooks fire, no Pub/Sub connections open

---

## Basic Usage

Consider an e-commerce application that processes orders, charges payments, and sends notifications:

```python
--8<-- "basic_usage/e9_01_inspect_basic.py:inspect_basic_subscribers"
```

Run the inspect command to see what your application registers:

```bash
fastpubsub inspect subscribers my_project.main:app
```

Output:

```
FastPubSub (v0.9.5): my_project.main:app — 3 subscribers

┌─────────────────────┬─────────────────┬──────────────────────┬─────────────────────┬───────────────────┐
│ Alias               │ Project         │ Topic                │ Subscription        │ Handler           │
├─────────────────────┼─────────────────┼──────────────────────┼─────────────────────┼───────────────────┤
│ charge-payments     │ ecommerce-prod  │ payment-events       │ payments-sub        │ charge_payment    │
│ process-orders      │ ecommerce-prod  │ order-events         │ orders-sub          │ process_order     │
│ send-notifications  │ ecommerce-prod  │ notification-events  │ notifications-sub   │ send_notification │
└─────────────────────┴─────────────────┴──────────────────────┴─────────────────────┴───────────────────┘
```

Subscribers are sorted alphabetically by alias.

---

## Selecting Columns

By default, the table shows five columns: **alias**, **project**, **topic**, **subscription**, and **handler**. Use `--columns` (or `-c`) to choose exactly which columns appear.

### Core Columns

These are shown by default when no `--columns` flag is provided:

| Column | Description |
|--------|-------------|
| `alias` | The subscriber's unique identifier, including router prefixes |
| `project` | The Google Cloud project ID |
| `topic` | The Pub/Sub topic name |
| `subscription` | The Pub/Sub subscription name |
| `handler` | The Python function name that processes messages |

### Policy Columns

These are available when you need deeper visibility into subscriber configuration:

| Column | Description |
|--------|-------------|
| `ack_deadline` | Acknowledgement deadline in seconds |
| `filter` | Pub/Sub filter expression |
| `ordering` | Whether message ordering is enabled |
| `exactly_once` | Whether exactly-once delivery is enabled |
| `max_messages` | Maximum concurrent messages in flight |
| `retry_min` | Minimum retry backoff delay in seconds |
| `retry_max` | Maximum retry backoff delay in seconds |
| `dead_letter_topic` | Dead letter topic name (empty if none) |
| `dead_letter_max_attempts` | Max delivery attempts before dead-lettering |
| `autocreate` | Whether topics/subscriptions are auto-created on startup |
| `autoupdate` | Whether subscriptions are auto-updated on startup |

### Examples

Show only alias and topic:

```bash
fastpubsub inspect subscribers my_project.main:app --columns alias,topic
```

Show all columns including policy fields:

```bash
fastpubsub inspect subscribers my_project.main:app --columns all
```

Show specific policy columns alongside the alias:

```bash
fastpubsub inspect subscribers my_project.main:app -c alias,topic,dead_letter_topic,exactly_once
```

!!! note

    Column names are case-insensitive and extra whitespace is ignored. `--columns " Alias , Topic "` works the same as `--columns alias,topic`.

---

## JSON Output

Use `--format json` (or `-f json`) for machine-readable output. This is useful for scripting, CI pipelines, or feeding into other tools.

```bash
fastpubsub inspect subscribers my_project.main:app --format json
```

```json
{
  "app": "my_project.main:app",
  "version": "0.9.5",
  "subscribers": [
    {
      "alias": "charge-payments",
      "project": "ecommerce-prod",
      "topic": "payment-events",
      "subscription": "payments-sub",
      "handler": "charge_payment"
    }
  ]
}
```

The `--columns` flag works with JSON too — it controls which fields appear in each subscriber object:

```bash
# Extract just aliases with jq
fastpubsub inspect subscribers my_project.main:app -f json | jq '.subscribers[].alias'

# Get a compact alias-to-topic mapping
fastpubsub inspect subscribers my_project.main:app -f json -c alias,topic \
  | jq '.subscribers[] | "\(.alias) → \(.topic)"'
```

---

## Filtering Subscribers

Use `--filter` to show only subscribers whose alias matches a glob pattern. This is useful when your application has many subscribers and you want to focus on a specific group.

```bash
# Show only order-related subscribers
fastpubsub inspect subscribers my_project.main:app --filter 'process-*'

# Multiple patterns (union)
fastpubsub inspect subscribers my_project.main:app --filter 'process-*' --filter 'send-*'
```

The `--filter` flag supports the same glob patterns as `run --subscribers`:

| Wildcard | Meaning |
|----------|---------|
| `*` | Matches any characters within a single segment (between dots) |
| `?` | Matches exactly one character |
| `**` | Matches zero or more dot-separated segments |

!!! tip "Dry-run your subscriber selection"

    Use `inspect subscribers --filter` to **preview which subscribers a pattern matches** before passing the same pattern to `run --subscribers`. This way you can verify your pattern selects exactly the right subscribers — without starting the server.

    ```bash
    # Step 1: Check which subscribers the pattern matches
    fastpubsub inspect subscribers my_project.main:app --filter 'platform.orders.*'

    # Step 2: Looks good — now run with the same pattern
    fastpubsub run my_project.main:app -s 'platform.orders.*'
    ```

---

## Inspecting Apps with Routers

When your application uses [routers](routers.md) with prefixes, subscriber aliases are prefixed automatically. The inspect command reveals the full resolved alias, making it easy to understand the hierarchy.

Consider a SaaS platform with nested routers:

```python
--8<-- "basic_usage/e9_02_inspect_routers.py:inspect_router_subscribers"
```

```bash
fastpubsub inspect subscribers saas.main:app
```

```
FastPubSub (v0.9.5): saas.main:app — 3 subscribers

┌──────────────────────────────┬──────────────────────┬──────────────────┬──────────────────┬────────────────┐
│ Alias                        │ Project              │ Topic            │ Subscription     │ Handler        │
├──────────────────────────────┼──────────────────────┼──────────────────┼──────────────────┼────────────────┤
│ platform.analytics.track     │ analytics-warehouse  │ analytics-events │ ...track-sub     │ track_event    │
│ platform.orders.fulfill      │ saas-platform        │ order-events     │ ...fulfill-sub   │ fulfill_order  │
│ platform.orders.invoice      │ saas-platform        │ order-events     │ ...invoice-sub   │ create_invoice │
└──────────────────────────────┴──────────────────────┴──────────────────┴──────────────────┴────────────────┘
```

Notice how:

- The `platform` prefix from the top-level router appears first in every alias
- The `orders` and `analytics` prefixes from the child routers follow
- The `track` subscriber shows a different project (`analytics-warehouse`) — the inspect command makes cross-project subscriptions visible at a glance

!!! tip

    The aliases shown by `inspect` are the same strings you pass to `--subscribers` when running the app. Use `inspect` to find the exact alias, then copy it into your `run` command.

---

## Recap

- **Command**: `fastpubsub inspect subscribers module:app` lists all subscribers
- **Default columns**: alias, project, topic, subscription, handler
- **Custom columns**: `--columns alias,topic` replaces the default set; `--columns all` shows everything
- **Filtering**: `--filter 'pattern'` shows only matching subscribers — use it as a dry-run for `run --subscribers`
- **JSON output**: `--format json` for scripting and CI pipelines
- **No side effects**: the command imports your app but does not start the server or connect to Pub/Sub
- **Router prefixes**: aliases reflect the full router prefix hierarchy
- **Sorted output**: subscribers are always sorted alphabetically by alias
