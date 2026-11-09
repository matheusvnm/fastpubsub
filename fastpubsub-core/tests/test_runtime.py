"""Tests of the network thread setting of the core runtime.

The runtime is global to the process and starts one time, so each test
runs the core in a new Python process.
"""

import os
import subprocess
import sys
import textwrap

import pytest

UNREACHABLE_ENDPOINT = "http://127.0.0.1:9"


def run_core(network_threads: str, script: str) -> str:
    environment = {**os.environ, "FASTPUBSUB_NETWORK_THREADS": network_threads}
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(script)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    return completed.stdout.strip()


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc")
def test_network_threads_variable_sets_the_worker_threads() -> None:
    output = run_core(
        "3",
        f"""
        import os
        import fastpubsub_core

        fastpubsub_core.AdminClient("{UNREACHABLE_ENDPOINT}")
        names = [
            open(f"/proc/self/task/{{task}}/comm").read().strip()
            for task in os.listdir("/proc/self/task")
        ]
        print(sum(name.startswith("fastpubsub-netw") for name in names))
        """,
    )

    assert output == "3"


@pytest.mark.parametrize("network_threads", ["0", "many"])
def test_wrong_network_threads_value_raises_core_error(
    network_threads: str,
) -> None:
    output = run_core(
        network_threads,
        f"""
        import fastpubsub_core

        try:
            fastpubsub_core.AdminClient("{UNREACHABLE_ENDPOINT}")
        except fastpubsub_core.CoreError as error:
            print(error)
        """,
    )

    assert "FASTPUBSUB_NETWORK_THREADS" in output
