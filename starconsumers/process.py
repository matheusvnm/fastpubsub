import multiprocessing
import socket
from typing import Any

import psutil
from pydantic import BaseModel

from starconsumers import observability
from starconsumers.datastructures import (
    WrappedTask,
)
from starconsumers.logger import logger
from starconsumers.pubsub.publisher import PubSubPublisher
from starconsumers.pubsub.subscriber import PubSubSubscriber
from starconsumers.pubsub.utils import check_credentials


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


class ProcessManager:
    def __init__(self) -> None:
        multiprocessing.set_start_method(method="spawn", force=True)
        self.processes: dict[str, multiprocessing.Process] = {}

    def spawn(self, tasks: list[WrappedTask]) -> None:
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
    def _spawn(task: WrappedTask) -> None:
        ProcessManager._start_apm_provider()
        subscriber = PubSubSubscriber()
        if not subscriber.create_subscription(task.subscription):
            if task.subscription.lifecycle_policy.autoupdate:
                subscriber.update_subscription(task.subscription)

        subscriber.subscribe(
            project_id=task.subscription.project_id,
            subscription_name=task.subscription.name,
            control_flow_policy=task.subscription.control_flow_policy,
            callback=task.handler,
        )

    @staticmethod
    def _create_topics(tasks: list[WrappedTask]) -> None:
        created_topics = set()
        for task in tasks:
            lifecycle_policy = task.subscription.lifecycle_policy
            if not lifecycle_policy.autocreate:
                logger.info(f"No autocreate enabled for {task.subscription.name}")
                continue

            key = task.subscription.project_id + ":" + task.subscription.topic_name
            if key in created_topics:
                continue

            logger.info(f"We will try to create the topic {key}")
            publisher = PubSubPublisher(
                project_id=task.subscription.project_id, topic_name=task.subscription.topic_name
            )
            publisher.create_topic()
            created_topics.add(key)

            dead_letter_policy = task.subscription.dead_letter_policy
            if not dead_letter_policy:
                continue

            key = task.subscription.project_id + ":" + dead_letter_policy.topic_name
            if key in created_topics:
                continue

            logger.info(
                f"We will try to create the dead letter topic {dead_letter_policy.topic_name}"
            )
            publisher = PubSubPublisher(
                project_id=task.subscription.project_id, topic_name=dead_letter_policy.topic_name
            )
            publisher.create_topic()
            created_topics.add(key)

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
