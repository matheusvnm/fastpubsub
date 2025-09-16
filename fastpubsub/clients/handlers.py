# TODO: Create the callback handler
# The mission is to receive a subscriber and apply its middlewares and handlers
# Also it should add instrumentation basic handlers
# Also its should add basic exception handling
# Also it should serialiaze the message as Message
# It should also build the middleware stack
# It should also handle

import asyncio
from typing import Any

from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import logger
from fastpubsub.pubsub.subscriber import Subscriber


class CallbackHandler:
    def __init__(self, subscriber: Subscriber):
        self.subscriber = subscriber

    def handle(self, message: PubSubMessage) -> None:
        coroutine = self._handle(message)
        asyncio.run(main=coroutine)

    async def _handle(self, message: PubSubMessage):
        topic_name = self.subscriber.topic_name
        subscription_name = self.subscriber.subscription_name

        with logger.contextualize(
            topic_name=topic_name,
            subscription_name=subscription_name,
            message_id=message.message_id,
        ):
            try:
                try:
                    response = await self._consume(message)
                    message.ack()
                    logger.info("Message successfully processed.")
                    return response
                except Drop:
                    logger.info("Message will be dropped.")
                    message.ack()
                    return
                except Retry:
                    logger.warning("Message processing will be retried later.")
                    message.nack()
                    return
                except Exception:
                    logger.exception("Unhandled exception on message", stacklevel=5)
                    message.nack()
                    return
            except AcknowledgeError:
                logger.exception("We failed to ack/nack the message", stacklevel=5)
                return

    async def _consume(self, message: PubSubMessage) -> Any:
        callback = self.subscriber.callback
        new_message = self._translate_message(message)
        return await callback.on_message(new_message)

    def _translate_message(self, message: PubSubMessage) -> Message:
        delivery_attempt = 0
        if message.delivery_attempt is not None:
            delivery_attempt = message.delivery_attempt

        return Message(
            id=message.message_id,
            size=message.size,
            data=message.data,
            attributes=message.attributes,
            delivery_attempt=delivery_attempt,
        )
