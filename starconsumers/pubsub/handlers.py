# TODO: Create the callback handler
# The mission is to receive a subscriber and apply its middlewares and handlers
# Also it should add instrumentation basic handlers
# Also its should add basic exception handling
# Also it should serialiaze the message as Message
# It should also build the middleware stack
# It should also handle

import asyncio
import traceback
from typing import Any

from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from starconsumers.exceptions import Drop, Retry
from starconsumers.logger import logger
from starconsumers.subscriber import Subscriber


class CallbackHandler:
    def __init__(self, subscriber: Subscriber):
        self.subscriber = subscriber

    def handle(self, message: PubSubMessage) -> None:
        topic_name = self.subscriber.topic_name
        subscription_name = self.subscriber.subscription_name

        with logger.contextualize(topic_name=topic_name, subscription_name=subscription_name):
            try:
                try:
                    response = self._consume(message)
                    message.ack()
                    logger.info(f"Message {message.message_id} successfully processed.")
                    return response
                except Drop:
                    logger.info(f"Message {message.message_id} will be dropped")
                    message.ack()
                    return
                except Retry:
                    logger.warning(f"Message {message.message_id} processing will be retried")
                    message.nack()
                    return
                except Exception:
                    logger.critical(traceback.format_exc())
                    logger.critical(f"Unhandled exception on message {message.message_id}")
                    message.nack()
                    return
            except AcknowledgeError:
                logger.critical(f"We failed to ack/nack the message {message.message_id}")
                return

    def _consume(self, message: PubSubMessage):
        callback = self.subscriber.handler
        for middleware in reversed(self.subscriber.middlewares):
            callback = middleware(callback)

        loop = asyncio.new_event_loop()
        return loop.run_until_complete(callback(message))

    def _deserialize_message(message) -> Any:
        pass
        # TODO: Add logic of deserialização
        # TODO:
