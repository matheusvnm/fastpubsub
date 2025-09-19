import os
from multiprocessing.connection import Connection
from threading import Thread

from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture

from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.concurrency.ipc import (
    InfoRequest,
    InterprocessRequest,
    LivenessProbeRequest,
    ReadinessProbeRequest,
    SubscriberInfo,
)
from fastpubsub.concurrency.utils import get_process_info
from fastpubsub.logger import logger
from fastpubsub.pubsub.subscriber import Subscriber


class SubscriberWorker:
    """A worker that runs a single Pub/Sub subscriber."""

    def __init__(self, subscriber: Subscriber, connection: Connection):
        self.subscriber = subscriber
        self.parent_connection = connection

        self._last_received_timestamp: float | None = None
        self._streaming_pull_future: StreamingPullFuture | None = None

    def get_readiness_status(self) -> bool:
        """Gets the current readiness status."""
        return self._streaming_pull_future.running()

    def run(self):
        """Starts the command handler and then the Pub/Sub subscription."""
        command_thread = Thread(target=self._command_loop, daemon=True)
        command_thread.start()

        client = PubSubSubscriberClient()
        with client.subscribe(self.subscriber) as future:
            self._streaming_pull_future = future

    def _command_loop(self):
        """A reusable blocking loop that waits for and handles requests."""
        while True:
            try:
                request: InterprocessRequest = self.parent_connection.recv()
                self._handle_request(request)
            except (EOFError, ConnectionResetError):
                logger.debug(f"Parent connection for the process {os.getpid()} closed. Exiting.")
                break
            except Exception as e:
                logger.exception(f"Error in command loop for PID {os.getpid()}: {e}")
                break

    def _handle_request(self, request: InterprocessRequest) -> None:
        """Dispatches the received request to the appropriate handler method."""
        if isinstance(request, LivenessProbeRequest):
            return self._handle_liveness_probe(request)
        elif isinstance(request, ReadinessProbeRequest):
            return self._handle_readiness_probe(request)
        elif isinstance(request, InfoRequest):
            return self._handle_info_request(request)

        logger.warning(f"The worker has no handler for {type(request).__name__}")

    def _handle_liveness_probe(self, request: LivenessProbeRequest):
        self.parent_connection.send(True)

    def _handle_readiness_probe(self, request: ReadinessProbeRequest):
        self.parent_connection.send(self.get_readiness_status())

    def _handle_info_request(self, request: InfoRequest):
        info = SubscriberInfo(process_info=get_process_info())
        self.parent_connection.send(info)

    @property
    def name(self) -> str:
        """Returns the subscriber's configured name."""
        return self.subscriber.name
