"""The compiled Pub/Sub transport of FastPubSub.

The module receives and publishes messages on a tokio runtime that runs in
its own threads. It sends the results to the asyncio event loop in batches,
through a ``Bridge``.

The ``fastpubsub`` package is the only expected user. The names here can
change in each release of ``fastpubsub-core``.
"""

import asyncio
from collections.abc import Callable
from typing import Any, Self, TypedDict, final

__all__ = [
    "AckHandle",
    "AdminClient",
    "AlreadyExistsError",
    "Bridge",
    "CoreError",
    "NotFoundError",
    "Publisher",
    "Subscription",
]

type _ReceivedEntry = tuple[str, bytes, dict[str, str], int, AckHandle]
type _PublishEntry = tuple[asyncio.Future[str], bool, Any]
type _CallbackBatch = list[tuple[Callable[[list[Any]], None], list[Any]]]

class SubscriptionSettings(TypedDict, total=False):
    """The settings of a subscription that ``AdminClient`` creates or updates.

    The type exists only for type checkers. At runtime, pass a ``dict``. A
    missing key uses the default value of the Pub/Sub service.

    Attributes:
        filter: The filter expression for the message attributes.
        ack_deadline_seconds: The time that a subscriber has to ack a
            message before Pub/Sub sends it again.
        enable_message_ordering: Deliver the messages with the same
            ordering key in the publish order.
        enable_exactly_once_delivery: Deliver each message one time only.
        min_backoff_seconds: The minimum delay before Pub/Sub sends a
            nacked message again.
        max_backoff_seconds: The maximum delay before Pub/Sub sends a
            nacked message again. A value of 0 disables the retry policy.
        dead_letter_topic: The full path of the topic that receives the
            messages that fail too many times. An empty value disables
            the dead-letter policy.
        max_delivery_attempts: The number of delivery attempts before
            Pub/Sub sends the message to the dead-letter topic.
    """

    filter: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: bool
    min_backoff_seconds: int
    max_backoff_seconds: int
    dead_letter_topic: str
    max_delivery_attempts: int

class CoreError(RuntimeError):
    """An error of the Pub/Sub transport.

    The message contains the error text from the Pub/Sub client.
    """

class NotFoundError(CoreError):
    """The topic or the subscription does not exist."""

class AlreadyExistsError(CoreError):
    """The topic or the subscription exists already."""

@final
class AckHandle:
    """The acknowledgement of one received message.

    The first call to ``ack`` or ``nack`` has an effect. The next calls do
    nothing. The methods do not block and do not need the GIL.
    """

    def ack(self) -> None:
        """Acknowledge the message, so Pub/Sub does not send it again."""

    def nack(self) -> None:
        """Reject the message, so Pub/Sub sends it again soon."""

@final
class Bridge:
    """Send the results of the transport to one asyncio event loop.

    A ``Bridge`` starts one thread with the name ``fastpubsub-bridge``. The
    thread collects the received messages and the publish results in
    batches of at most ``max_batch_size`` items. For each batch, it holds
    the GIL one time and calls
    ``call_soon_threadsafe(run_callbacks, batch)``. The event loop then
    calls ``run_callbacks`` with a list of ``(callback, entries)`` pairs.

    Create one ``Bridge`` for each event loop. ``Subscription`` and
    ``Publisher`` objects of the same loop share it.
    """

    def __new__(
        cls,
        max_batch_size: int,
        call_soon_threadsafe: Callable[..., Any],
        run_callbacks: Callable[[_CallbackBatch], None],
    ) -> Self:
        """Start the bridge thread.

        Args:
            max_batch_size: The maximum number of items in one call to the
                event loop. A value of 0 is changed to 1.
            call_soon_threadsafe: The ``call_soon_threadsafe`` method of
                the target event loop.
            run_callbacks: The function that the event loop calls with each
                batch.

        Raises:
            CoreError: The operating system cannot start the thread.
        """

@final
class Subscription:
    """One streaming pull on a Pub/Sub subscription.

    The stream starts in the constructor and runs on the tokio runtime. It
    sends each received message to ``dispatch`` through the ``Bridge``, as
    a ``(message_id, data, attributes, delivery_attempt, ack_handle)``
    tuple. The handler must call ``ack_handle.ack()`` or
    ``ack_handle.nack()`` for each message.
    """

    def __new__(
        cls,
        subscription_path: str,
        max_messages: int,
        bridge: Bridge,
        dispatch: Callable[[list[_ReceivedEntry]], None],
        endpoint: str | None = None,
    ) -> Self:
        """Connect to Pub/Sub and start the stream.

        Args:
            subscription_path: The full subscription path, in the format
                ``projects/<project>/subscriptions/<name>``.
            max_messages: The maximum number of handlers that the client
                runs at the same time. The server gets twice this limit,
                so the next messages are ready when a handler stops.
            bridge: The bridge of the event loop that runs ``dispatch``.
            dispatch: The function that receives each batch of messages.
            endpoint: The URL of the Pub/Sub emulator. ``None`` connects
                to the Pub/Sub service with the default credentials.

        Raises:
            ValueError: The path has a wrong format, or ``max_messages``
                is 0.
            CoreError: The client cannot connect or cannot get the
                credentials.
        """

    def is_running(self) -> bool:
        """Check if the stream runs.

        Returns:
            True until the stream stops because of an error or a shutdown.
        """

    def shutdown(self, timeout_seconds: float) -> bool:
        """Stop the stream and wait for the pending acks.

        The call blocks, but it does not hold the GIL.

        Args:
            timeout_seconds: The maximum time to wait, in seconds.

        Returns:
            True if the stream stopped before the timeout.
        """

@final
class Publisher:
    """Publish messages to one Pub/Sub topic.

    The client collects the messages in batches before it sends them. The
    result of each publish arrives at ``resolve_batch`` through the
    ``Bridge``, as a ``(future, is_success, value)`` tuple. ``value`` is
    the message ID when ``is_success`` is true, or an exception when it is
    false.
    """

    def __new__(
        cls,
        topic_path: str,
        bridge: Bridge,
        resolve_batch: Callable[[list[_PublishEntry]], None],
        endpoint: str | None = None,
    ) -> Self:
        """Connect to Pub/Sub.

        Args:
            topic_path: The full topic path, in the format
                ``projects/<project>/topics/<name>``.
            bridge: The bridge of the event loop that runs
                ``resolve_batch``.
            resolve_batch: The function that receives each batch of
                publish results.
            endpoint: The URL of the Pub/Sub emulator. ``None`` connects
                to the Pub/Sub service with the default credentials.

        Raises:
            ValueError: The path has a wrong format.
            CoreError: The client cannot connect or cannot get the
                credentials.
        """

    def publish(
        self,
        data: bytes,
        attributes: dict[str, str],
        future: asyncio.Future[str],
        ordering_key: str | None = None,
    ) -> None:
        """Add one message to the next batch.

        The call does not block. The result arrives later in ``future``,
        through ``resolve_batch``.

        Args:
            data: The message payload.
            attributes: The message attributes.
            future: The future that receives the message ID or the error.
            ordering_key: The ordering key. ``None`` or an empty string
                publishes without a key.

        Raises:
            CoreError: The publisher is closed.
        """

    def flush(self) -> None:
        """Send all pending batches and wait for the results.

        The call blocks, but it does not hold the GIL.

        Raises:
            CoreError: The tokio runtime did not start.
        """

@final
class AdminClient:
    """Create, update and remove topics and subscriptions.

    Each method blocks until Pub/Sub answers, but it does not hold the
    GIL. Call the methods from a worker thread in async code.
    """

    def __new__(cls, endpoint: str | None = None) -> Self:
        """Connect to Pub/Sub.

        Args:
            endpoint: The URL of the Pub/Sub emulator. ``None`` connects
                to the Pub/Sub service with the default credentials.

        Raises:
            CoreError: The client cannot connect or cannot get the
                credentials.
        """

    def create_topic(self, topic_path: str) -> None:
        """Create a topic.

        Args:
            topic_path: The full topic path.

        Raises:
            ValueError: The path has a wrong format.
            AlreadyExistsError: The topic exists already.
            CoreError: Pub/Sub returned a different error.
        """

    def delete_topic(self, topic_path: str) -> None:
        """Remove a topic.

        Args:
            topic_path: The full topic path.

        Raises:
            ValueError: The path has a wrong format.
            NotFoundError: The topic does not exist.
            CoreError: Pub/Sub returned a different error.
        """

    def create_subscription(
        self,
        subscription_path: str,
        topic_path: str,
        settings: SubscriptionSettings,
    ) -> None:
        """Create a subscription on a topic.

        Args:
            subscription_path: The full subscription path.
            topic_path: The full path of the topic to subscribe to.
            settings: The subscription settings.

        Raises:
            ValueError: A path has a wrong format.
            AlreadyExistsError: The subscription exists already.
            NotFoundError: The topic does not exist.
            CoreError: Pub/Sub returned a different error, or a setting
                has a value out of range.
        """

    def update_subscription(
        self,
        subscription_path: str,
        topic_path: str,
        settings: SubscriptionSettings,
        update_fields: list[str],
    ) -> None:
        """Change the settings of a subscription.

        Args:
            subscription_path: The full subscription path.
            topic_path: The full path of the topic of the subscription.
            settings: The new subscription settings.
            update_fields: The names of the Pub/Sub fields to change, for
                example ``ack_deadline_seconds`` or ``retry_policy``.

        Raises:
            ValueError: A path has a wrong format.
            NotFoundError: The subscription does not exist.
            CoreError: Pub/Sub returned a different error, or a setting
                has a value out of range.
        """

    def delete_subscription(self, subscription_path: str) -> None:
        """Remove a subscription.

        Args:
            subscription_path: The full subscription path.

        Raises:
            ValueError: The path has a wrong format.
            NotFoundError: The subscription does not exist.
            CoreError: Pub/Sub returned a different error.
        """
