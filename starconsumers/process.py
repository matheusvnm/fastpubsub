import multiprocessing
import socket
from typing import Any

import psutil

from starconsumers import observability
from starconsumers.datastructures import (
    ProcessInfo,
    ProcessSocketConnection,
    ProcessSocketConnectionAddress,
    Task,
)
from starconsumers.logger import logger
from starconsumers.pubsub.auth import check_credentials
from starconsumers.pubsub.publisher import PubSubPublisher
from starconsumers.pubsub.subscriber import PubSubSubscriber


def spawn(task: Task) -> None:
    subscriber = PubSubSubscriber()
    subscriber.create_subscription(task.subscription)
    subscriber.subscribe(task.subscription.project_id, task.subscription.name, task.handler)


class ProcessManager:
    def __init__(self) -> None:
        multiprocessing.set_start_method(method="spawn", force=True)
        self.processes: dict[str, multiprocessing.Process] = {}

    def spawn(self, tasks: list[Task]) -> None:
        check_credentials()
        ProcessManager._start_apm_provider()
        ProcessManager._create_topics(tasks)
        for task in tasks:
            process = multiprocessing.Process(
                target=ProcessManager._spawn, args=(task,), daemon=True
            )
            self.processes[task.subscription.name] = process
            self.processes[task.subscription.name].start()

    @staticmethod
    def _spawn(task: Task) -> None:
        ProcessManager._start_apm_provider()
        subscriber = PubSubSubscriber()
        subscriber.create_subscription(task.subscription)
        subscriber.subscribe(task.subscription.project_id, task.subscription.name, task.handler)

    @staticmethod
    def _create_topics(tasks: list[Task]) -> None:
        created_topics = set()
        for task in tasks:
            key = task.subscription.project_id + ":" + task.subscription.topic_name
            if not task.autocreate or key in created_topics:
                logger.debug(
                    f"No auto topic create or already created topic for {task.subscription.name}"
                )
                continue

            logger.info(f"We will try to create the topic {key}")
            created_topics.add(key)
            publisher = PubSubPublisher(
                project_id=task.subscription.project_id, topic_name=task.subscription.topic_name
            )
            publisher.create_topic()

    @staticmethod
    def _start_apm_provider() -> None:
        apm = observability.get_apm_provider()
        apm.initialize()

    def terminate(self) -> None:
        children_processes = psutil.Process().children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        _, alive_processes = psutil.wait_procs(children_processes, timeout=5)
        for alive_process in alive_processes:
            alive_process.kill()

    def probe_processes(self) -> dict[str, Any]:
        apm = observability.get_apm_provider()
        response: dict[str, Any] = {
            "apm": {"status": apm.active(), "provides": apm.__class__.__name__}
        }

        processes_infos = []
        for name, process in self.processes.items():
            process_info = self._get_process_info(id=process.pid, name=name)
            processes_infos.append(process_info)

        response.update({"processes": processes_infos})
        return response

    def _get_process_info(self, id: int | None, name: str) -> dict[str, Any]:
        connections: list[ProcessSocketConnection] = []

        try:
            process = psutil.Process(id)
        except psutil.NoSuchProcess:
            content = ProcessInfo(name=name, connections=connections)
            return content.model_dump()

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

        content = ProcessInfo(
            name=name,
            connections=connections,
            running=process.is_running(),
            num_threads=process.num_threads(),
        )

        return content.model_dump()
