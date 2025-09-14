import multiprocessing
import os
import socket

import psutil
from pydantic import BaseModel

from fastpubsub import observability
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.logger import logger
from fastpubsub.subscriber import Subscriber


class ProcessSocketConnectionAddress(BaseModel):
    ip: str
    port: int
    hostname: str


class ProcessSocketConnection(BaseModel):
    status: str
    address: ProcessSocketConnectionAddress | None


class ProcessInfo(BaseModel):
    name: str
    num_threads: int = 0
    running: bool = False
    connections: list[ProcessSocketConnection] = []


class APMInfo(BaseModel):
    active: bool
    ready: bool
    provider: str


class ProbeResponse(BaseModel):
    apm: APMInfo
    processes: list[ProcessInfo]


def _spawn(subscriber: Subscriber) -> None:
    logger.info(
        f"Started the subscriber {subscriber.subscription_name} subscription subprocess [{os.getpid()}]"
    )
    apm = observability.get_apm_provider()
    apm.initialize()

    client = PubSubSubscriberClient()
    client.subscribe(subscriber)


class ProcessManager:
    def __init__(self) -> None:
        self.context = multiprocessing.get_context(method="spawn")
        self.processes: dict[str, multiprocessing.Process] = {}

    def spawn(self, subscriber: Subscriber) -> None:
        # TODO: Remover sistema daemonic do processo
        # TODO: Adicionar algum tipo de tratamento on terminate.
        process = self.context.Process(target=_spawn, args=(subscriber,), daemon=True)
        self.processes[subscriber.subscription_name] = process
        self.processes[subscriber.subscription_name].start()

    def terminate(self, subscriber: Subscriber) -> None:
        logger.info(f"The subscriber {subscriber.subscription_name} child process will terminate")

        process = self.processes[subscriber.subscription_name]
        children_processes = psutil.Process(pid=process.pid).children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        _, alive_processes = psutil.wait_procs(children_processes, timeout=5)
        for alive_process in alive_processes:
            alive_process.kill()

    def probe(self) -> ProbeResponse:
        apm = observability.get_apm_provider()
        apm_info = APMInfo(
            active=apm.active(), ready=bool(apm.get_trace_id()), provider=apm.__class__.__name__
        )

        processes_infos = []
        for name, process in self.processes.items():
            process_info = self._get_process_info(id=process.pid, name=name)
            processes_infos.append(process_info)

        return ProbeResponse(apm=apm_info, processes=processes_infos)

    def _get_process_info(self, id: int | None, name: str) -> ProcessInfo:
        connections: list[ProcessSocketConnection] = []

        try:
            process = psutil.Process(id)
        except psutil.NoSuchProcess:
            return ProcessInfo(name=name, connections=connections)

        try:
            for connection in process.net_connections():
                if not (connection.raddr):
                    continue

                hostname, _, _ = socket.gethostbyaddr(connection.raddr.ip)
                address = ProcessSocketConnectionAddress(
                    ip=connection.raddr.ip, port=connection.raddr.port, hostname=hostname
                )
                connection = ProcessSocketConnection(
                    address=address,
                    status=connection.status,
                )

                connections.append(connection)
        except psutil.AccessDenied:
            logger.warning("We lack the permissions to get connection infos.")

        return ProcessInfo(
            name=name,
            connections=connections,
            running=process.is_running(),
            num_threads=process.num_threads(),
        )
