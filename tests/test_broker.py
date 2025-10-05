import os
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from fastpubsub.baserouter import BaseRouter
from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException

BROKER_MODULE_PATH = "fastpubsub.broker"

class TestPubSubBroker:

    @pytest.fixture
    def process_controller(self) -> Generator[MagicMock]:
        with patch(f"{BROKER_MODULE_PATH}.ProcessController") as mock:
            yield mock.return_value

    @pytest.fixture
    def subscription_builder(self) -> Generator[MagicMock]:
        with patch(f"{BROKER_MODULE_PATH}.PubSubSubscriptionBuilder") as mock:
            yield mock.return_value


    @pytest.mark.parametrize(
        "invalid_project_id",
        [
            None,
            "   ",
            "",
            213,
        ],
    )
    def test_init_with_invalid_project_id_raises_exception(self, invalid_project_id):
        with pytest.raises(FastPubSubException):
            PubSubBroker(project_id=invalid_project_id)

    def test_init_with_valid_project_id_succeeds(self):
        broker = PubSubBroker(project_id="my-valid-project-id")
        assert broker.router.project_id == "my-valid-project-id"

    def test_include_router_with_invalid_type_raises_exception(self):
        class SomeOtherRouter(BaseRouter):
            pass

        broker = PubSubBroker(project_id="cloud-project")
        with pytest.raises(FastPubSubException):
            broker.include_router(SomeOtherRouter())

    # Parametrize
    # No subscriber
    # No envvar
    # with subscriber

    @pytest.mark.parametrize(
        ["selected_subscribers",
         "expected_subscribers"],
        [
            (
                "",
                ["a", "b"],
            ),
            (
                "a,b",
                ["a", "b"],
            ),
            (
                " a , b ",
                ["a", "b"],
            ),
            (
                "a",
                ["a"],
            ),
            (
                "a,c",
                ["a"],
            )
        ],
    )
    def test_filter_subscribers(
        self, 
        selected_subscribers: str,
        expected_subscribers: list[str],
        broker: PubSubBroker, 
    ):
        os.environ["FASTPUBSUB_SUBSCRIBERS"] = selected_subscribers

        mock_router = MagicMock()
        mock_router._get_subscribers = MagicMock(return_value={"a": "a", "b": "b"})
        broker.router = mock_router

        found_subscribers = broker._filter_subscribers()
        assert len(found_subscribers) == len(expected_subscribers)
        for expected_sub in expected_subscribers:
            if not expected_sub in found_subscribers:
                pytest.fail(reason=f"The expected subscribers do not match the filtered ones {expected_subscribers} {found_subscribers}")

    @pytest.mark.asyncio
    async def test_shutdown_successfully(self, process_controller: MagicMock, broker: PubSubBroker):
        await broker.shutdown()
        process_controller.terminate.assert_called_once()


    def test_start_broker(self): ...
    
    def test_readiness_probe(self): ...

    def test_liveness_probe(self): ...

    def test_info_probe(self, process_controller: MagicMock, broker: PubSubBroker): 
        expected_info = {"some": "data"}
        process_controller.get_info.return_value = expected_info
        info = broker.info()
        assert info == expected_info
