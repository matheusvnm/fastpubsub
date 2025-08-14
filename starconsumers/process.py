
import multiprocessing

import psutil

from starconsumers.consumer import Task



def spawn(task: Task):
    subscriber = PubSubSubscriber()
    subscriber.create_subscription(subscription=task.subscription, autocreate=task.autocreate)
    subscriber.subscribe(task.handler)


class ProcessManager:

    def __init__(self):
        multiprocessing.set_start_method(method="spawn", force=True)
        self.processes: dict[str, multiprocessing.Process] = {}

    def spawn(self, tasks: list[Task]):
        for task in tasks:
            process = multiprocessing.Process(target=spawn, args=(task,), daemon=True)
            self.processes[task] = process


    def terminate(self):
        children_processes = psutil.Process().children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        _, alive_processes = psutil.wait_procs(children_processes, timeout=5)
        for alive_process in alive_processes:
            alive_process.kill()
