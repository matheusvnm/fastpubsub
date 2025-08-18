
import multiprocessing

import psutil

from starconsumers.consumer import Task
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
        self._create_topics(tasks)
        for task in tasks:
            process = multiprocessing.Process(target=self._spawn, args=(task,), daemon=True)
            self.processes[task.subscription.name] = process
            self.processes[task.subscription.name].start()

    @staticmethod
    def _spawn(task: Task):
        subscriber = PubSubSubscriber()
        subscriber.create_subscription(task.subscription)
        subscriber.subscribe(task.subscription.project_id, task.subscription.name, task.handler)

    @staticmethod
    def _create_topics(tasks: list[Task]):
        created_topics = set()
        for task in tasks:
            key = task.subscription.project_id + "/" + task.subscription.topic_name
            if not task.autocreate or key in created_topics:
                continue
            
            created_topics.add(key)
            publisher = PubSubPublisher(project_id=task.subscription.project_id, topic_name=task.subscription.topic_name)
            publisher.create_topic()    

    def terminate(self):
        children_processes = psutil.Process().children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        _, alive_processes = psutil.wait_procs(children_processes, timeout=5)
        for alive_process in alive_processes:
            alive_process.kill()
