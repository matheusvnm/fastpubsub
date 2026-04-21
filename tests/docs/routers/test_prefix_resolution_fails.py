import pytest

from docs.snippets.routers.e2_03_prefix_resolution_fails import (
    broker as first_broker,
)
from docs.snippets.routers.e3_03_prefix_resolution_fails import (
    broker as second_broker,
)
from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException


class TestAliasResolutionFailed:
    @pytest.mark.parametrize(
        "broker",
        [
            pytest.param(first_broker, id="duplicate_alias"),
            pytest.param(second_broker, id="cross_level_duplicate_alias"),
        ],
    )
    @pytest.mark.docs
    def test_alias_conflict_raises_error(self, broker: PubSubBroker) -> None:
        with pytest.raises(FastPubSubException) as exc_info:
            broker.router.get_subscribers()

        error_message = str(exc_info.value)
        assert "test-alias-abc" in error_message
        assert "conflict" in error_message.lower()
