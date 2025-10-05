import contextlib
from dataclasses import dataclass
from multiprocessing import ProcessError
from multiprocessing.connection import Connection
from multiprocessing.context import DefaultContext
from multiprocessing.process import BaseProcess
from time import sleep, time
from typing import Any

from fastpubsub.concurrency.ipc import (
    InfoRequest,
    InfoResponse,
    InterprocessRequest,
    LivenessProbeRequest,
    LivenessProbeResponse,
    ReadinessProbeRequest,
    ReadinessProbeResponse,
    WorkerInfo,
)
from fastpubsub.concurrency.utils import get_process_info
from fastpubsub.concurrency.workers.subscriber import SubscriberWorker
from fastpubsub.logger import logger
from fastpubsub.pubsub.subscriber import Subscriber


def start_subscriber_worker(subscriber: Subscriber, connection: Connection) -> None:
    """Target function to create and run a SubscriberWorker in a new process."""
    worker = SubscriberWorker(subscriber, connection)
    worker.run()


@dataclass
class ManagedProcess:
    """A data structure linking a process and its connection."""

    process: BaseProcess
    connection: Connection
    subscriber_name: str


class ManagerWorker:
    """A worker that manages a fleet of SubscriberWorkers."""

    def __init__(
        self, subscribers: list[Subscriber], connection: Connection, mp_context: DefaultContext
    ) -> None:
        self.subscribers = subscribers
        self.parent_connection = connection
        self.mp_context = mp_context
        self._managed_processes: dict[int, ManagedProcess] = {}

    def run(self) -> None:
        """The main entry point for the manager process."""
        self._startup()
        with contextlib.suppress(KeyboardInterrupt):
            while True:
                logger.debug("Checking the subscribers state")
                self._poll_for_requests()
                if self._should_restart():
                    self._restart()
                sleep(1)

    def _startup(self) -> None:
        """Initializes and starts all configured subscriber workers."""
        for subscriber in self.subscribers:
            self._create_subscriber_worker(subscriber)

    def _create_subscriber_worker(self, subscriber: Subscriber) -> None:
        """Creates, starts, and registers a single SubscriberWorker."""
        parent_conn, child_conn = self.mp_context.Pipe()
        process = self.mp_context.Process(
            target=start_subscriber_worker,
            args=(subscriber, child_conn),
        )

        process.start()
        if not process.pid:
            raise ProcessError(f"We could not start the subscriber process for '{subscriber.name}'")

        managed_process = ManagedProcess(
            process=process, connection=parent_conn, subscriber_name=subscriber.name
        )
        self._managed_processes[process.pid] = managed_process

    def _poll_for_requests(self) -> None:
        """Non-blocking check for requests from the parent controller."""
        if self.parent_connection.poll():
            try:
                request: InterprocessRequest = self.parent_connection.recv()
                self._handle_request(request)
            except (EOFError, ConnectionResetError) as e:
                raise SystemExit("Parent connection closed. Exiting ManagerWorker.") from e

    def _handle_request(self, request: InterprocessRequest) -> None:
        """Dispatches the received request to the appropriate handler method."""
        if isinstance(request, LivenessProbeRequest):
            return self._handle_liveness_probe(request)
        elif isinstance(request, ReadinessProbeRequest):
            return self._handle_readiness_probe(request)
        elif isinstance(request, InfoRequest):
            return self._handle_info_request(request)

        logger.warning(f"The worker has no handler for {type(request).__name__}")

    def _propagate_request_and_collect(
        self, request: InterprocessRequest, timeout: float
    ) -> dict[str, Any]:
        """Helper to send a request to all children and gather responses."""
        pending_procs = {p.process.pid: p for p in list(self._managed_processes.values())}
        final_results: dict[str, Any] = {}
        for pid, managed_proc in list(pending_procs.items()):
            try:
                managed_proc.connection.send(request)
            except (BrokenPipeError, EOFError):
                final_results[managed_proc.subscriber_name] = None
                del pending_procs[pid]

        deadline = time() + timeout
        while pending_procs and time() < deadline:
            for pid, managed_proc in list(pending_procs.items()):
                if managed_proc.connection.poll(0.05):
                    try:
                        final_results[managed_proc.subscriber_name] = managed_proc.connection.recv()
                        del pending_procs[pid]
                    except (EOFError, BrokenPipeError):
                        del pending_procs[pid]

        return final_results

    def _handle_liveness_probe(self, request: LivenessProbeRequest) -> None:
        results = self._propagate_request_and_collect(request, timeout=request.timeout)
        self.parent_connection.send(LivenessProbeResponse(results=results))

    def _handle_readiness_probe(self, request: ReadinessProbeRequest) -> None:
        results = self._propagate_request_and_collect(request, timeout=10.0)
        self.parent_connection.send(ReadinessProbeResponse(results=results))

    def _handle_info_request(self, request: InfoRequest) -> None:
        subscriber_infos = self._propagate_request_and_collect(request, timeout=10.0)
        worker_info = WorkerInfo(
            process_info=get_process_info(),
            subscribers=subscriber_infos,
        )
        self.parent_connection.send(InfoResponse(worker_info=worker_info))

    def _should_restart(self) -> bool:
        """Checks if any managed process has died."""
        return not all(p.process.is_alive() for p in self._managed_processes.values())

    def _restart(self) -> None:
        """Finds and restarts any dead subscriber workers."""
        for pid, managed_proc in list(self._managed_processes.items()):
            if managed_proc.process.is_alive():
                continue

            logger.error(
                f"Subscriber '{managed_proc.subscriber_name}' (pid:{pid}) is dead. Restarting."
            )
            subscriber = next(s for s in self.subscribers if s.name == managed_proc.subscriber_name)
            self._create_subscriber_worker(subscriber)
            del self._managed_processes[pid]
