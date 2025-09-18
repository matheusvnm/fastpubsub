import contextlib
import multiprocessing
import os
import signal
import socket
from time import sleep

import psutil

from fastpubsub import observability
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.logger import logger
from fastpubsub.pubsub.subscriber import Subscriber


class SubscriberProcess:
    def __init__(self, subscriber: Subscriber):
        self.subscriber = subscriber
        self.process = multiprocessing.Process(
            target=self._spawn,
        )

    def start(self):
        self.process.start()

    def _spawn(self):
        logger.debug(f"The observability provider will start for subprocess [{os.getpid()}].")
        apm = observability.get_apm_provider()
        apm.initialize()

        client = PubSubSubscriberClient()
        logger.debug(f"The message listener will start for subprocess [{os.getpid()}].")
        client.subscribe(self.subscriber)

    def alive(self) -> bool:
        return self.process.is_alive()

    @property
    def name(self) -> str:
        return self.subscriber.name

    @property
    def id(self) -> int:
        return self.process.pid

    @property
    def exitcode(self) -> int:
        return self.process.exitcode

    def killed(self) -> bool:
        return self.exitcode and abs(self.exitcode) == signal.SIGKILL

    def kill(self):
        with contextlib.suppress(psutil.NoSuchProcess):
            process = psutil.Process(pid=self.id)
            children_processes = process.children(recursive=True)
            for child_process in children_processes:
                child_process.kill()

            process.kill()


class ProcessManager:
    def __init__(self) -> None:
        self.subscribers: list[Subscriber] = []

        multiprocessing.set_start_method(method="spawn", force=True)
        self.process = multiprocessing.Process(
            target=self._run,
        )

    def add_subscriber(self, subscriber: Subscriber):
        self.subscribers.append(subscriber)

    def start(self):
        self.process.start()

    def _run(self):
        self._startup()
        with contextlib.suppress(KeyboardInterrupt):
            while True:
                logger.debug("Checking if we should restart the processes.")
                if self._should_restart():
                    self._restart()
                sleep(1)

    def _startup(self):
        self.subscribers_processes: list[SubscriberProcess] = []
        for subscriber in self.subscribers:
            subscriber_process = SubscriberProcess(subscriber=subscriber)
            self.subscribers_processes.append(subscriber_process)

        for subscriber_process in self.subscribers_processes:
            subscriber_process.start()

    def _should_restart(self) -> bool:
        return not all(p.alive() for p in self.subscribers_processes)

    def _restart(self):
        logger.debug("Some dead processes detected. We will try to revive them.")

        active_subscribers = []
        for subscriber_process in self.subscribers_processes:
            if subscriber_process.alive():
                active_subscribers.append(subscriber_process)
                continue

            message = "Subscriber %s (pid:%s) exited with code %s."
            if subscriber_process.killed():
                message += " Perhaps out of memory?"

            logger.error(
                message, subscriber_process.name, subscriber_process.id, subscriber_process.exitcode
            )
            if not subscriber_process.killed():
                subscriber_process.kill()

            new_subscriber_process = self._restart_process(subscriber_process.subscriber)
            logger.info("Restarted the subscriber as child process [%s]", new_subscriber_process.id)
            active_subscribers.append(new_subscriber_process)

        self.subscribers_processes = active_subscribers

    def _restart_process(self, subscriber: Subscriber) -> SubscriberProcess:
        new_subscriber_process = SubscriberProcess(subscriber=subscriber)
        new_subscriber_process.start()
        return new_subscriber_process

    def terminate(self):
        process = psutil.Process(pid=self.process.pid)

        logger.debug(f"We received a signal to finish the execution of process [{process.pid}].")
        children_processes = process.children(recursive=True)
        for child_process in children_processes:
            child_process.terminate()

        process.terminate()
        _, alive_processes = psutil.wait_procs([*children_processes, process], timeout=10)
        for alive_process in alive_processes:
            alive_process.kill()

    def probe(self):
        if not self.process.is_alive():
            logger.info("Its not alive!")

        processes = psutil.Process(pid=self.process.pid).children(recursive=True)
        for process in processes:
            logger.info(f"We probed the process {process.pid}")
            try:
                for connection in process.net_connections():
                    if not (connection.raddr):
                        continue
                    hostname, _, _ = socket.gethostbyaddr(connection.raddr.ip)
                    logger.info(
                        f"The process {process.pid} is connected to {hostname} with status {connection.status}"
                    )
            except psutil.AccessDenied:
                logger.warning("We lack the permissions to get connection infos.")
