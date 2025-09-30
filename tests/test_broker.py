import pytest

from fastpubsub.baserouter import BaseRouter
from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException


class TestPubSubBroker:
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


# For probes (alivre, ready, info):
# 1. Test alive with subscribers:
# 2. Test alive without subscribers
# 3. Test not alive


# Test get selected subscribers
# 1. No subscribers
# 2. No clean subscriber
# 3. All subscris cleaned
