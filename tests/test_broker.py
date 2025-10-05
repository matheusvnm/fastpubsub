import os
from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from fastpubsub.baserouter import BaseRouter
from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.pubsub.subscriber import Subscriber

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

    @pytest.mark.parametrize(
        ["selected_subscribers", "expected_subscribers"],
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
            ),
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
            if expected_sub not in found_subscribers:
                pytest.fail(
                    reason="The expected subscribers do not "
                    f"match the filtered ones {expected_subscribers} {found_subscribers}"
                )

    @pytest.mark.asyncio
    async def test_shutdown_successfully(self, process_controller: MagicMock, broker: PubSubBroker):
        await broker.shutdown()
        process_controller.terminate.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_broker_no_sub_error(self, broker: PubSubBroker):
        broker._filter_subscribers = lambda: []
        with pytest.raises(FastPubSubException):
            await broker.start()

    @pytest.mark.asyncio
    async def test_start_broker(
        self, subscription_builder: MagicMock, process_controller: MagicMock, broker: PubSubBroker
    ):
        expected_subscriber = MagicMock(spec=Subscriber)
        broker._filter_subscribers = lambda: [expected_subscriber]
        await broker.start()

        subscription_builder.build.assert_called_once_with(expected_subscriber)
        process_controller.add_subscriber.assert_called_once_with(expected_subscriber)
        process_controller.start.assert_called_once()

    @pytest.mark.parametrize(
        ["response", "expected_readiness"],
        [
            (
                {"sub_a": True},
                True,
            ),
            (
                {},
                False,
            ),
            (
                {"sub_a": True, "sub_b": False},
                False,
            ),
        ],
    )
    def test_readiness_probe(
        self,
        response: dict[str, bool],
        expected_readiness: bool,
        process_controller: MagicMock,
        broker: PubSubBroker,
    ):
        readiness_call = process_controller.get_readiness
        readiness_call.return_value = response

        response = broker.ready()
        assert response == expected_readiness

    @pytest.mark.parametrize(
        ["response", "expected_liveness"],
        [
            (
                {"sub_a": True},
                True,
            ),
            (
                {},
                False,
            ),
            (
                {"sub_a": True, "sub_b": False},
                False,
            ),
        ],
    )
    def test_liveness_probe(
        self,
        response: dict[str, bool],
        expected_liveness: bool,
        process_controller: MagicMock,
        broker: PubSubBroker,
    ):
        liveness_call = process_controller.get_liveness
        liveness_call.return_value = response

        response = broker.alive()
        assert response == expected_liveness

    def test_info_probe(self, process_controller: MagicMock, broker: PubSubBroker):
        expected_info = {"some": "data"}
        process_controller.get_info.return_value = expected_info
        info = broker.info()
        assert info == expected_info
