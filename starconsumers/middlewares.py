


import asyncio
from dataclasses import dataclass
import traceback
from typing import Union

from fastapi.types import DecoratedCallable
from starconsumers.exceptions import AckException, NackException
from starconsumers.observability import apm

from starconsumers.datastructures import MessageMiddleware
from google.cloud.pubsub_v1.subscriber.message import Message



class BasicExceptionHandler(MessageMiddleware):

    def __call__(self, message: Message):
        try:
            print("I'm BasicExceptionHandlerMiddleware")
            response = super().__call__(message)
            message.ack()
            print(f"Message {message.message_id} successfully processed.")
            return response
        except AckException as ex:
            print(f"ACK: Message {message.message_id} ended as processed")
            message.ack()
            return
        except NackException:
            print(f"NACK: Message {message.message_id} processing failed")
            message.nack()
            return
        except Exception:
            print(traceback.format_exc())
            print(f"Unhandled exception on message {message.message_id}")
            message.nack()
            return

class APMTransactionMiddleware(MessageMiddleware):

    def __call__(self, message: Message):
        with apm.background_transaction(name="transaction"):
            apm.set_distributed_trace_context(message.attributes)
            return super().__call__(message)

class APMTracePropagationMiddleware(MessageMiddleware):
    def __call__(self, message: Message):
        apm.set_distributed_trace_context(message.attributes)
        return super().__call__(message)


class AsyncContextMiddleware(MessageMiddleware):

    def __init__(self, next_call: Union["MessageMiddleware", DecoratedCallable]):
        self.next_call = next_call
        self.is_coroutine = asyncio.iscoroutinefunction(next_call) 

    def __call__(self, message: Message):
        print("Hi I'm AsyncContextMiddleware")
        if not self.is_coroutine:
            return super().__call__(message)

        return asyncio.run(super().__call__(message))