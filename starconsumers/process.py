
import multiprocessing
import socket

import psutil

from starconsumers import observability
from starconsumers.consumer import Task
from starconsumers.datastructures import ProcessInfo, ProcessSocketConnection, ProcessSocketConnectionAddress
from starconsumers.pubsub.publisher import PubSubPublisher
from starconsumers.pubsub.subscriber import PubSubSubscriber



def spawn(task: Task):
    subscriber = PubSubSubscriber(task.subscription)
    subscriber.create_subscription()
    subscriber.subscribe(task.handler)

class ProcessManager:

    def __init__(self):
        multiprocessing.set_start_method(method="spawn", force=True)
        self.processes: dict[str, multiprocessing.Process] = {}

    def spawn(self, tasks: list[Task]):
        ProcessManager._start_apm_provider()
        ProcessManager._create_topics(tasks)
        for task in tasks:
            process = multiprocessing.Process(target=ProcessManager._spawn, args=(task,), daemon=True)
            self.processes[task.subscription.name] = process
            self.processes[task.subscription.name].start()

    @staticmethod
    def _spawn(task: Task):
        ProcessManager._start_apm_provider()
        subscriber = PubSubSubscriber()
        subscriber.create_subscription(task.subscription)
        subscriber.subscribe(task.subscription.project_id, task.subscription.name, task.handler)

    @staticmethod
    def _create_topics(tasks: list[Task]):
        created_topics = set()
        for task in tasks:
            key = task.subscription.project_id + ":" + task.subscription.topic_name
            if not task.autocreate or key in created_topics:
                print(f"No auto topic create or already created topic for {task.subscription.name}")
                continue
            
            print(f"We will try to create the topic {key}")
            created_topics.add(key)
            publisher = PubSubPublisher(project_id=task.subscription.project_id, topic_name=task.subscription.topic_name)
            publisher.create_topic()    

    @staticmethod
    def _start_apm_provider():
        apm = observability.get_apm_provider()
        apm.initialize()

    def terminate(self):
        children_processes = psutil.Process().children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        _, alive_processes = psutil.wait_procs(children_processes, timeout=5)
        for alive_process in alive_processes:
            alive_process.kill()


    def probe_processes(self) -> dict:
        apm = observability.get_apm_provider()
        response = {"apm": {"status": apm.active(), "provides": apm.__class__.__name__}}

        response["processes"] = []
        for name, process in self.processes.items():
            process_info = self._get_process_info(id=process.pid, name=name)
            response["processes"].append(process_info)

        return response
    

    def _get_process_info(self, id: int, name: str) -> ProcessInfo:
        connections = []

        try:
            process = psutil.Process(id)
        except psutil.NoSuchProcess:
            return ProcessInfo(name=name, connections=connections)

        try:
            for connection in process.net_connections():
                if not (connection.raddr):
                    return

                hostname, _, _ = socket.gethostbyaddr(connection.raddr.ip)
                address = ProcessSocketConnectionAddress(
                    ip=connection.raddr.ip, 
                    port=connection.raddr.port,
                    hostname=hostname
                    
                )
                connection = ProcessSocketConnection(
                    address=address,
                    status=connection.status,
                )

                connections.append(connection)
        except psutil.AccessDenied:
            print("We lack the permissions to get connection infos.")

        return ProcessInfo(
            name=name,
            connections=connections,
            running=process.is_running(),
            num_threads=process.num_threads(),
        )
