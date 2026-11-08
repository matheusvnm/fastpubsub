"""Tests of the argument checks in the Python bindings of the core.

The checks run before a network call, so the tests need no emulator.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastpubsub_core import AdminClient, Bridge, Subscription

UNREACHABLE_ENDPOINT = "http://127.0.0.1:9"
TOPIC_PATH = "projects/demo/topics/orders"
SUBSCRIPTION_PATH = "projects/demo/subscriptions/orders"


@pytest.fixture
def admin_client() -> AdminClient:
    return AdminClient(UNREACHABLE_ENDPOINT)


def test_topic_path_with_a_wrong_format_raises_value_error(
    admin_client: AdminClient,
) -> None:
    with pytest.raises(ValueError, match="projects/<project>/topics/<name>"):
        admin_client.create_topic("orders")


def test_dead_letter_topic_with_a_wrong_format_raises_value_error(
    admin_client: AdminClient,
) -> None:
    with pytest.raises(ValueError, match="topics"):
        admin_client.create_subscription(
            SUBSCRIPTION_PATH, TOPIC_PATH, {"dead_letter_topic": "orders-dlt"}
        )


def test_setting_with_a_wrong_type_raises_type_error(
    admin_client: AdminClient,
) -> None:
    settings: Any = {"ack_deadline_seconds": "sixty"}

    with pytest.raises(TypeError):
        admin_client.create_subscription(
            SUBSCRIPTION_PATH, TOPIC_PATH, settings
        )


def test_zero_max_messages_raises_value_error() -> None:
    bridge = Bridge(1, MagicMock(), MagicMock())

    with pytest.raises(ValueError, match="max_messages"):
        Subscription(
            SUBSCRIPTION_PATH,
            0,
            bridge,
            MagicMock(),
            UNREACHABLE_ENDPOINT,
        )
