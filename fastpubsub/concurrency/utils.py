import inspect
import socket
from collections.abc import Callable
from types import FunctionType
from typing import ParamSpec, TypeVar

import psutil

from fastpubsub.concurrency.ipc import ConnectionInfo, ProcessInfo
from fastpubsub.logger import logger
from fastpubsub.middlewares.base import BaseMiddleware

P = ParamSpec("P")
T = TypeVar("T")


def ensure_async_callable(obj: Callable[P, T]):
    if isinstance(obj, FunctionType):
        if not inspect.iscoroutinefunction(obj):
            raise TypeError(f"The function {obj} must be async.")
        return

    if issubclass(obj, BaseMiddleware):
        if not (
            inspect.iscoroutinefunction(obj.on_message)
            and inspect.iscoroutinefunction(obj.on_publish)
        ):
            raise TypeError(f"The on_message and on_publish from class {obj} must be async.")
        return

    if inspect.isclass(obj):
        if not callable(obj):
            raise TypeError(f"The class {obj} must implement a async def __call__.")

        if not inspect.iscoroutinefunction(obj.__call__):
            raise TypeError(f"The class {obj} __call__ function must be async.")
        return
    return


def get_process_info() -> ProcessInfo:
    """Gathers detailed information about the current process, including connections."""
    p = psutil.Process()
    connections_info = []
    with p.oneshot():
        try:
            for conn in p.net_connections(kind="tcp"):
                if conn.status == psutil.CONN_ESTABLISHED and conn.raddr:
                    hostname = "N/A"
                    try:
                        hostname, _, _ = socket.gethostbyaddr(conn.raddr.ip)
                    except (socket.herror, socket.gaierror):
                        hostname = f"RDNS_FAILED ({conn.raddr.ip})"
                    connections_info.append(
                        ConnectionInfo(
                            remote_address=conn.raddr.ip,
                            remote_port=conn.raddr.port,
                            hostname=hostname,
                            status=conn.status,
                        )
                    )
        except psutil.AccessDenied:
            logger.warning(f"Could not get network connections for PID {p.pid} due to permissions.")

        return ProcessInfo(
            pid=p.pid,
            running=p.is_running(),
            num_threads=p.num_threads(),
            connections=connections_info,
        )
