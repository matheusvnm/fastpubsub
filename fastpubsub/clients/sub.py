import asyncio
import os
from contextlib import suppress
from datetime import timedelta
from typing import Any

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.types import FlowControl
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription

from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry, StarConsumersException
from fastpubsub.logger import logger
from fastpubsub.observability import get_apm_provider
from fastpubsub.pubsub.subscriber import Subscriber


class CallbackHandler:
    def __init__(self, subscriber: Subscriber):
        self.subscriber = subscriber

    def handle(self, message: PubSubMessage) -> None:
        apm = get_apm_provider()

        with apm.background_transaction(name=self.subscriber.name):
            apm.set_distributed_trace_context(message.attributes)
            context = {
                "span_id": apm.get_span_id(),
                "trace_id": apm.get_trace_id(),
                "messsage_id": message.message_id,
                "topic_name": self.subscriber.topic_name,
                "subscription_name": self.subscriber.subscription_name,
            }

            with logger.contextualize(**context):
                try:
                    try:
                        new_message = self._translate_message(message)
                        response = self._consume(new_message)
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

    def _consume(self, message: Message) -> Any:
        callback = self.subscriber.callback
        coroutine = callback.on_message(message)
        return asyncio.run(main=coroutine)


class PubSubSubscriberClient:
    def __init__(self):
        self.is_emulator = True if os.getenv("PUBSUB_EMULATOR_HOST") else False

    def _create_subscription_request(self, subscriber: Subscriber) -> Subscription:
        name = SubscriberClient.subscription_path(
            subscriber.project_id, subscriber.subscription_name
        )
        topic = SubscriberClient.topic_path(subscriber.project_id, subscriber.topic_name)

        dlt_policy = None
        if subscriber.dead_letter_policy:
            dlt_topic = SubscriberClient.topic_path(
                subscriber.project_id,
                subscriber.dead_letter_policy.topic_name,
            )
            dlt_policy = DeadLetterPolicy(
                dead_letter_topic=dlt_topic,
                max_delivery_attempts=subscriber.dead_letter_policy.max_delivery_attempts,
            )

        min_backoff_delay = timedelta(seconds=subscriber.retry_policy.min_backoff_delay_secs)
        max_backoff_delay = timedelta(seconds=subscriber.retry_policy.max_backoff_delay_secs)
        retry_policy = RetryPolicy(
            minimum_backoff=min_backoff_delay, maximum_backoff=max_backoff_delay
        )

        return Subscription(
            name=name,
            topic=topic,
            retry_policy=retry_policy,
            dead_letter_policy=dlt_policy,
            filter=subscriber.delivery_policy.filter_expression,
            ack_deadline_seconds=subscriber.delivery_policy.ack_deadline_seconds,
            enable_message_ordering=subscriber.delivery_policy.enable_message_ordering,
            enable_exactly_once_delivery=subscriber.delivery_policy.enable_exactly_once_delivery,
        )

    def create_subscription(self, subscriber: Subscriber) -> bool:
        """
        Creates the Pub/Sub subscription if it doesn't exist.
        Handles AlreadyExists errors gracefully.
        """
        subscription_request = self._create_subscription_request(subscriber=subscriber)

        with suppress(AlreadyExists):
            try:
                with SubscriberClient() as client:
                    logger.debug(f"Attempting to create subscription: {subscription_request.name}")
                    client.create_subscription(request=subscription_request)
                    logger.debug(f"Successfully created subscription: {subscription_request.name}")
                    return True
            except NotFound:
                raise StarConsumersException(
                    f"The topic {subscription_request.topic} was not found for subscription"
                )

    def update_subscription(self, subscriber: Subscriber) -> None:
        subscription_request = self._create_subscription_request(subscriber=subscriber)
        update_fields = [
            "ack_deadline_seconds",
            "dead_letter_policy",
            "retry_policy",
            "enable_exactly_once_delivery",
        ]

        if not self.is_emulator:
            update_fields.append("filter")

        update_mask = FieldMask(paths=update_fields)
        with SubscriberClient() as client:
            logger.debug(f"Attempting to update the subscription: {subscription_request.name}")
            response = client.update_subscription(
                subscription=subscription_request, update_mask=update_mask
            )
            logger.debug(f"Successfully updated the subscription: {subscription_request.name}")
            logger.debug(f"The subscription is now following the configuration: {response}")

    def subscribe(self, subscriber: Subscriber) -> None:
        """
        Starts listening for messages on the configured Pub/Sub subscription.
        This method is blocking and will run indefinitely.
        """
        subscription_path = SubscriberClient.subscription_path(
            subscriber.project_id, subscriber.subscription_name
        )
        callback_handler = CallbackHandler(subscriber)

        with SubscriberClient() as client:
            logger.debug(f"Listening for messages on {subscription_path}")
            streaming_pull_future = client.subscribe(
                subscription_path,
                await_callbacks_on_shutdown=True,
                callback=callback_handler.handle,
                flow_control=FlowControl(
                    max_messages=subscriber.control_flow_policy.max_messages,
                    max_bytes=subscriber.control_flow_policy.max_bytes,
                ),
            )
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                logger.debug(f"Subscriber '{subscriber.subscription_name}' stopped by user")
            except Exception:
                logger.exception(
                    "Subscription stream terminated "
                    f"unexpectedly for '{subscriber.subscription_name}'",
                    stacklevel=5,
                )
            finally:
                logger.debug(
                    f"Sending cancel streaming pull command for '{subscriber.subscription_name}'."
                )
                streaming_pull_future.cancel()
                logger.debug(f"Subscriber '{subscriber.subscription_name}' has shutdown.")
