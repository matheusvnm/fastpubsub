from fastpubsub import FastPubSub, Message, PubSubBroker, PubSubRouter
from fastpubsub.logger import logger

from .posts_domain import posts_router
from .user_domain import users_router


# --8<-- [start:main_app]
broker = PubSubBroker(project_id="fastpubsub-pubsub-local")

# Include the routers as part of the broker
broker.include_router(users_router)
broker.include_router(posts_router)

app = FastPubSub(broker)
# --8<-- [end:main_app]


# --8<-- [start:router_publish]
@app.after_startup
async def publish_test_messages():
    await users_router.publish("users-topic", data={"username": "Yugi"})
    await posts_router.publish("posts-topic", data={"title": "My New Post"})
# --8<-- [end:router_publish]
