"""Tests for free-threaded Python builds."""

import asyncio
import os
import subprocess
import sys
import sysconfig
import threading

import pytest
from fastpubsub_core import Bridge

from fastpubsub._internal.event_loop_bridge import EventLoopBridge

IS_FREE_THREADED_BUILD = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


@pytest.mark.skipif(
    not IS_FREE_THREADED_BUILD, reason="needs a free-threaded Python"
)
def test_imports_keep_the_gil_disabled() -> None:
    environment = {
        name: value
        for name, value in os.environ.items()
        if name != "PYTHON_GIL"
    }
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, fastpubsub, fastpubsub_core, fastpubsub.testing;"
            "print(sys._is_gil_enabled())",
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )

    assert completed.stdout.strip() == "False"


def test_each_event_loop_thread_gets_its_own_bridge() -> None:
    thread_count = 8
    bridges: list[Bridge] = []
    lock = threading.Lock()
    start = threading.Barrier(thread_count)

    async def get_bridge_twice() -> None:
        first = EventLoopBridge.get()
        second = EventLoopBridge.get()
        assert first is second
        with lock:
            bridges.append(first)

    def run_loop() -> None:
        start.wait()
        asyncio.run(get_bridge_twice())

    EventLoopBridge.clear()
    threads = [threading.Thread(target=run_loop) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    EventLoopBridge.clear()

    assert len({id(bridge) for bridge in bridges}) == thread_count
