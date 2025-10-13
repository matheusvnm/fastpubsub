# Routers

Routers are a powerful feature that allows you to divide your application into smaller, more manageable pieces. They are especially useful for large applications with many subscribers and publishers, allowing you to organize your code by domain and to avoid naming conflicts.

When you create a router, you can specify a prefix that will be applied to all the subscribers and publishers in that router. This prefix will be part of the subscriber's alias, which is used by the CLI to select which subscribers to run. For example, if you have a router with the prefix `users`, you can run all the subscribers in that router by using the alias `users.*`.

## How to use routers

The **FastPubSub** routers can be used to organize your subscribers and publishers.
You can create a hierarchy of routers by including routers in other routers.

## Creating a router hierarchy

Here's an example of how to create a router hierarchy:

```python
from fastpubsub import PubSubBroker, PubSubRouter

users_router = PubSubRouter(prefix="users")
posts_router = PubSubRouter(prefix="posts")

@users_router.subscriber(
    alias="users-subscriber",
    topic_name="users-topic",
    subscription_name="users-subscription",
)
async def handle_user_message(message):
    print(message)

@posts_router.subscriber(
    alias="posts-subscriber",
    topic_name="posts-topic",
    subscription_name="posts-subscription",
)
async def handle_post_message(message):
    print(message)

broker = PubSubBroker(
    project_id="your-project-id",
    routers=[users_router, posts_router],
)
```

In this example, the `users-subscriber` will have the alias `users.users-subscriber` and the subscription name `users.users-subscription`.
The `posts-subscriber` will have the alias `posts.posts-subscriber` and the subscription name `posts.posts-subscription`.

## Nested routers

You can also nest routers to create a tree-like hierarchy.
This is useful for organizing your application into even smaller pieces.

Here's an example of how to nest routers:

```python
from fastpubsub import FastPubSub PubSubBroker, PubSubRouter, Message

banking_router = PubSubRouter(prefix="banking")
finance_router = PubSubRouter(prefix="finance")

core_router = PubSubRouter(prefix="core")
core_router.include_router(banking_router)
core_router.include_router(finance_router)


@core_router.subscriber(
    alias="core_handler",
    topic_name="one_topic",
    subscription_name="one_sub",
)
async def handle_message_core(message: Message):
    print(message)


@banking_router.subscriber(
    alias="banking_handler",
    topic_name="one_topic",
    subscription_name="one_sub_banking",
)
async def handle_message_banking(message: Message):
    print(message)


@finance_router.subscriber(
    alias="finance_handler",
    topic_name="one_topic",
    subscription_name="one_sub_finance",
)
async def handle_message_finance(message: Message):
    print(message)



broker = PubSubBroker(project_id="your-project-id")
broker.include_router(core_router)

app = FastPubSub(broker)


```

In this example, the aliases and subscription names would be:

- `handle_message_core`:
  - alias: `core.core_handler`
  - subscription name: `core.one_sub`
- `handle_message_banking`:
  - alias: `core.banking.banking_handler`
  - subscription name: `core.banking.one_sub_banking`
- `handle_message_finance`:
  - alias: `core.finance.finance_handler`
  - subscription name: `core.finance.one_sub_finance`
