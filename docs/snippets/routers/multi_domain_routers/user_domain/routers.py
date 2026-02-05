from fastpubsub import Message, PubSubRouter
from fastpubsub.logger import logger

users_router = PubSubRouter(prefix="users")


@users_router.subscriber(
    alias="created",
    topic_name="users-topic",
    subscription_name="users-subscription",
)
async def handle_user_message(message: Message):
    logger.info(f"Processing message {message.id} in users domain.")
