import contextlib
from dataclasses import asdict
from multiprocessing import get_context
from multiprocessing.connection import Connection
from multiprocessing.context import BaseContext, DefaultContext
from multiprocessing.process import BaseProcess
from typing import Any, cast

import psutil

from fastpubsub.concurrency.ipc import (
    InfoRequest,
    InfoResponse,
    InterprocessRequest,
    LivenessProbeRequest,
    LivenessProbeResponse,
    ReadinessProbeRequest,
    ReadinessProbeResponse,
)
from fastpubsub.concurrency.utils import get_process_info
from fastpubsub.concurrency.workers.manager import ManagerWorker
from fastpubsub.logger import logger
from fastpubsub.pubsub.subscriber import Subscriber


def start_manager_worker(
    subscribers: list[Subscriber], connection: Connection, mp_context: DefaultContext
) -> None:
    """Target function to create and run a ManagerWorker in a new process."""
    worker = ManagerWorker(subscribers, connection, mp_context)
    worker.run()


class ProcessController:
    """Public-facing controller for managing a fleet of subscriber processes."""

    def __init__(self) -> None:
        self._subscribers: list[Subscriber] = []
        self._connection: Connection | None = None
        self._manager_process: BaseProcess | None = None
        self._mp_context = get_context("spawn")

    def add_subscriber(self, subscriber: Subscriber) -> None:
        """Registers a subscriber configuration to be managed."""
        if self._manager_process and self._manager_process.is_alive():
            raise RuntimeError("Cannot add subscribers after the manager has started.")
        self._subscribers.append(subscriber)

    def start(self) -> None:
        """Starts the child manager process using a 'spawn' context."""
        if not self._subscribers:
            return

        parent_conn, child_conn = self._mp_context.Pipe()
        self._connection = parent_conn
        self._manager_process = self._mp_context.Process(
            target=start_manager_worker, args=(self._subscribers, child_conn, self._mp_context)
        )
        self._manager_process.start()
        logger.info(f"Started ManagerWorker process with PID: {self._manager_process.pid}")

    def _send_request(
        self, request: InterprocessRequest, timeout: float
    ) -> InterprocessRequest | None:
        if not (self._manager_process and self._manager_process.is_alive() and self._connection):
            return None

        try:
            self._connection.send(request)
            if self._connection.poll(timeout):
                response = self._connection.recv()
                response = cast(InterprocessRequest, response)
                return response
        except (BrokenPipeError, EOFError):
            logger.error("Pipe to manager process is broken.")        

        return None

    def get_liveness(self, timeout: float = 10.0) -> dict[str, bool]:
        """Checks if the managed subscriber processes are alive."""
        response = self._send_request(LivenessProbeRequest(timeout=timeout * 0.9), timeout=timeout)
        if isinstance(response, LivenessProbeResponse):
            return response.results
        return {}

    def get_readiness(self, timeout: float = 12.0) -> dict[str, bool]:
        """Checks if the subscribers are ready to process messages."""
        response = self._send_request(ReadinessProbeRequest(), timeout=timeout)
        if isinstance(response, ReadinessProbeResponse):
            return response.results
        return {}

    def get_info(self, timeout: float = 12.0) -> dict[str, Any]:
        """Gathers detailed information about the entire process hierarchy."""

        manager_info = None
        controller_info = asdict(get_process_info())
        response = self._send_request(InfoRequest(), timeout=timeout)
        if isinstance(response, InfoResponse):
            manager_info = asdict(response.worker_info)

        return {
            "controller": controller_info,
            "manager": manager_info,
        }

    def terminate(self) -> None:
        """Terminates the manager process and all its children gracefully."""
        if not self._manager_process or not self._manager_process.pid:
            return

        with contextlib.suppress(psutil.NoSuchProcess):
            parent = psutil.Process(self._manager_process.pid)
            children = parent.children(recursive=True)

            all_procs = [*children, parent]
            for p in all_procs:
                p.terminate()

            _, alive = psutil.wait_procs(all_procs, timeout=5)
            for p in alive:
                p.kill()
