


import asyncio
import traceback
from typing import Union
from starconsumers.logger import logger

from fastapi.types import DecoratedCallable
from starconsumers import observability
from starconsumers.exceptions import DropException, RetryException

from starconsumers.datastructures import MessageMiddleware, TopicMessage
from google.cloud.pubsub_v1.subscriber.message import Message




class BasicExceptionHandler(MessageMiddleware):

    def __call__(self, message: Message):
        try:
            response = super().__call__(message)
            message.ack()
            logger.info(f"Message {message.message_id} successfully processed.")
            return response
        except DropException as ex:
            logger.info(f"DROP: Message {message.message_id} will be dropped")
            message.ack()
            return
        except RetryException:
            logger.warning(f"RETRY: Message {message.message_id} will be retried")
            message.nack()
            return
        except Exception:
            logger.exception(f"Unhandled exception on message {message.message_id}", stacklevel=5)
            message.nack()
            return

class APMTransactionMiddleware(MessageMiddleware):

    def __call__(self, message: Message):
        apm = observability.get_apm_provider()
        with apm.background_transaction(name="MessageMiddleware"):
            apm.set_distributed_trace_context(message.attributes)
            return super().__call__(message)

class APMLogContextMiddleware(MessageMiddleware):

    def __call__(self, message: Message):
        apm = observability.get_apm_provider()

        trace_id = apm.get_trace_id()
        span_id = apm.get_span_id()
        with logger.contextualize(trace_id=trace_id, span_id=span_id):
            return super().__call__(message)

class MessageSerializerMiddleware(MessageMiddleware):
    def __call__(self, message: Message):
        serialized_message = TopicMessage(
            id=message.message_id,
            size=message.size,
            data=message.data,
            attributes=message.attributes,
            delivery_attempt=message.delivery_attempt,
        )

        return super().__call__(serialized_message)

class AsyncContextMiddleware(MessageMiddleware):

    def __init__(self, next_call: Union["MessageMiddleware", DecoratedCallable]):
        self.next_call = next_call
        self.is_coroutine = asyncio.iscoroutinefunction(next_call) 

    def __call__(self, message: TopicMessage):
        if not self.is_coroutine:
            return super().__call__(message)

        return asyncio.run(super().__call__(message))