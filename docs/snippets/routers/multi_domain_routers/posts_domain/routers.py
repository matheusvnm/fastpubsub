from fastpubsub import Message, PubSubRouter
from fastpubsub.logger import logger

posts_router = PubSubRouter(prefix="posts")


@posts_router.subscriber(
    alias="published",
    topic_name="posts-topic",
    subscription_name="posts-subscription",
)
async def handle_post_message(message: Message):
    logger.info(f"Processing message {message.id} in posts domain.")
