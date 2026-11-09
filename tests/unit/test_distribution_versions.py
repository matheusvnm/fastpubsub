"""Tests that fastpubsub and fastpubsub-core release with one version."""

import tomllib
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def read_project(relative_path: str) -> dict[str, Any]:
    text = (REPOSITORY_ROOT / relative_path).read_text()
    project: dict[str, Any] = tomllib.loads(text)["project"]
    return project


def test_core_version_matches_the_package_version() -> None:
    package = read_project("pyproject.toml")
    core = read_project("fastpubsub-core/pyproject.toml")

    assert core["version"] == package["version"]


def test_package_pins_the_core_of_the_same_version() -> None:
    package = read_project("pyproject.toml")

    assert f"fastpubsub-core=={package['version']}" in package["dependencies"]
