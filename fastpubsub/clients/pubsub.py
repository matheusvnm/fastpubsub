import os
from concurrent.futures import Future
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING

from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud.pubsub_v1 import PublisherClient
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1 import (
    DeadLetterPolicy as DLTPolicy,
)
from google.pubsub_v1 import (
    PublisherAsyncClient,
    ReceivedMessage,
    RetryPolicy,
    SubscriberAsyncClient,
    Subscription,
)

from fastpubsub import observability
from fastpubsub.datastructures import DeadLetterPolicy, MessageDeliveryPolicy, MessageRetryPolicy
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.logger import logger

if TYPE_CHECKING:
    pass


DEFAULT_PUBSUB_TIMEOUT = 10.0


class PubSubClient:
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.is_emulator = True if os.getenv("PUBSUB_EMULATOR_HOST") else False

    async def _create_subscription_request(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> Subscription:
        name = SubscriberAsyncClient.subscription_path(self.project_id, subscription_name)
        topic = SubscriberAsyncClient.topic_path(self.project_id, topic_name)

        dlt_policy = None
        if dead_letter_policy:
            dlt_topic = SubscriberAsyncClient.topic_path(
                self.project_id,
                dead_letter_policy.topic_name,
            )

            dlt_policy = DLTPolicy(
                dead_letter_topic=dlt_topic,
                max_delivery_attempts=dead_letter_policy.max_delivery_attempts,
            )

        min_backoff_delay = timedelta(seconds=retry_policy.min_backoff_delay_secs)
        max_backoff_delay = timedelta(seconds=retry_policy.max_backoff_delay_secs)
        message_retry_policy = RetryPolicy(
            minimum_backoff=min_backoff_delay, maximum_backoff=max_backoff_delay
        )

        return Subscription(
            name=name,
            topic=topic,
            dead_letter_policy=dlt_policy,
            retry_policy=message_retry_policy,
            filter=delivery_policy.filter_expression,
            ack_deadline_seconds=delivery_policy.ack_deadline_seconds,
            enable_message_ordering=delivery_policy.enable_message_ordering,
            enable_exactly_once_delivery=delivery_policy.enable_exactly_once_delivery,
        )

    async def create_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> None:
        async with SubscriberAsyncClient() as client:
            subscription_request = await self._create_subscription_request(
                topic_name=topic_name,
                subscription_name=subscription_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                dead_letter_policy=dead_letter_policy,
            )

            with suppress(AlreadyExists):
                logger.debug(f"Attempting to create subscription: {subscription_request.name}")
                await client.create_subscription(
                    request=subscription_request, timeout=DEFAULT_PUBSUB_TIMEOUT
                )
                logger.debug(f"Successfully created subscription: {subscription_request.name}")

    async def update_subscription(
        self,
        topic_name: str,
        subscription_name: str,
        retry_policy: MessageRetryPolicy,
        delivery_policy: MessageDeliveryPolicy,
        dead_letter_policy: DeadLetterPolicy | None = None,
    ) -> None:
        async with SubscriberAsyncClient() as client:
            subscription_request = await self._create_subscription_request(
                topic_name=topic_name,
                subscription_name=subscription_name,
                retry_policy=retry_policy,
                delivery_policy=delivery_policy,
                dead_letter_policy=dead_letter_policy,
            )

            update_fields = [
                "ack_deadline_seconds",
                "dead_letter_policy",
                "retry_policy",
                "enable_exactly_once_delivery",
            ]

            if not self.is_emulator:
                update_fields.append("filter")
            update_mask = FieldMask(paths=update_fields)

            try:
                logger.debug(f"Attempting to update the subscription: {subscription_request.name}")
                response = await client.update_subscription(
                    subscription=subscription_request,
                    update_mask=update_mask,
                    timeout=DEFAULT_PUBSUB_TIMEOUT,
                )
                logger.debug(f"Successfully updated the subscription: {subscription_request.name}")
                logger.debug(f"The subscription is now following the configuration: {response}")
            except NotFound as e:
                raise FastPubSubException(
                    "We could not update the subscription configuration. "
                    f"The topic {subscription_request.topic} or "
                    f"subscription {subscription_request.name} were not found. "
                    "They may be deleted or not autocreated. "
                    "Please, setup your @subscriber with the 'autocreate=True' "
                    "option to automatically create them."
                ) from e

    async def pull(self, subscription_name: str, max_messages: int) -> list[ReceivedMessage]:
        async with SubscriberAsyncClient() as client:
            subscription_path = client.subscription_path(self.project_id, subscription_name)

            response = await client.pull(
                subscription=subscription_path,
                timeout=DEFAULT_PUBSUB_TIMEOUT,
                max_messages=max_messages,
            )

        return list(response.received_messages)

    async def ack(self, ack_ids: list[str], subscription_name: str) -> None:
        async with SubscriberAsyncClient() as client:
            subscription_path = client.subscription_path(self.project_id, subscription_name)

            await client.acknowledge(
                subscription=subscription_path, ack_ids=ack_ids, timeout=DEFAULT_PUBSUB_TIMEOUT
            )

    async def nack(self, ack_ids: list[str], subscription_name: str) -> None:
        async with SubscriberAsyncClient() as client:
            subscription_path = client.subscription_path(self.project_id, subscription_name)

            await client.modify_ack_deadline(
                subscription=subscription_path,
                ack_ids=ack_ids,
                ack_deadline_seconds=0,
                timeout=DEFAULT_PUBSUB_TIMEOUT,
            )

    async def create_topic(self, topic_name: str, create_default_subscription: bool = True) -> None:
        async with PublisherAsyncClient() as publisher_client:
            with suppress(AlreadyExists):
                logger.debug(f"Creating topic '{topic_name}'.")
                topic_path = publisher_client.topic_path(self.project_id, topic_name)
                topic = await publisher_client.create_topic(name=topic_path)
                logger.debug(f"Created topic '{topic.name}' sucessfully.")

                if not create_default_subscription:
                    return

                async with SubscriberAsyncClient() as subscriber_client:
                    logger.debug(f"Creating default subscription for '{topic_path}'.")
                    default_subscription_path = subscriber_client.subscription_path(
                        self.project_id, topic_name
                    )
                    subscription = await subscriber_client.create_subscription(
                        name=default_subscription_path,
                        topic=topic_path,
                        timeout=DEFAULT_PUBSUB_TIMEOUT,
                    )
                    logger.debug(
                        "Creating default subscription created successfully for "
                        f"'{topic_path}' as {subscription.name}."
                    )

    def publish(
        self,
        topic_name: str,
        *,
        data: bytes,
        ordering_key: str,
        attributes: dict[str, str] | None,
    ) -> None:
        with PublisherClient(transport="") as client:
            topic_path = client.topic_path(self.project_id, topic_name)

            apm = observability.get_apm_provider()
            attributes = {} if attributes is None else attributes
            headers = apm.get_distributed_trace_context()
            headers.update(attributes)
            try:
                response: Future[str] = client.publish(
                    topic=topic_path,
                    data=data,
                    ordering_key=ordering_key,
                    timeout=DEFAULT_PUBSUB_TIMEOUT,
                    **headers,
                )

                message_id = response.result()
                logger.info(f"Message published for topic {topic_path} with id {message_id}")
                logger.debug(f"We sent {data!r} with metadata {attributes}")
            except Exception:
                logger.exception("Publisher failure", stacklevel=5)
                raise
