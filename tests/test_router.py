from unittest.mock import MagicMock

import pytest

from fastpubsub.exceptions import FastPubSubException
from fastpubsub.router import PubSubRouter


class TestPubSubRouter:
    def test_propagate_project_id(self):
        router = PubSubRouter()
        router._propagate_project_id("test-project")
        assert router.project_id == "test-project"

    def test_add_prefix(self):
        router = PubSubRouter(prefix="base")
        sub_router = PubSubRouter(prefix="sub")
        router.include_router(sub_router)
        assert sub_router.prefix == "base.sub"

    def test_realias_subscribers(self):
        router = PubSubRouter(prefix="api")

        @router.subscriber(alias="test.alias", topic_name="topic", subscription_name="sub")
        async def handler():
            pass

        subscribers = router._get_subscribers()
        assert "api.test.alias" in subscribers

    def test_include_wrong_type(self):
        router = PubSubRouter()
        with pytest.raises(FastPubSubException):
            router.include_router(MagicMock())

    def test_invalid_routers_type_constructor(self):
        with pytest.raises(FastPubSubException):
            PubSubRouter(routers=123)

    def test_invalid_middlewares_type_constructor(self):
        with pytest.raises(FastPubSubException):
            PubSubRouter(middlewares=321)

    def test_get_subscribers(self):
        router = PubSubRouter()

        @router.subscriber(alias="sub1", topic_name="topic1", subscription_name="sub1")
        async def handler1():
            pass

        @router.subscriber(alias="sub2", topic_name="topic2", subscription_name="sub2")
        async def handler2():
            pass

        subscribers = router._get_subscribers()
        assert len(subscribers) == 2
        assert "sub1" in subscribers
        assert "sub2" in subscribers

    def test_duplicate_prefix_error(self):
        router = PubSubRouter()
        sub_router1 = PubSubRouter(prefix="same")
        sub_router2 = PubSubRouter(prefix="same")
        router.include_router(sub_router1)
        with pytest.raises(FastPubSubException):
            router.include_router(sub_router2)

    def test_nested_router_prefixing(self):
        level3_router = PubSubRouter(prefix="level3")
        level2_router = PubSubRouter(prefix="level2", routers=(level3_router,))
        level1_router = PubSubRouter(prefix="level1", routers=(level2_router,))

        assert level1_router.prefix == "level1"
        assert level2_router.prefix == "level1.level2"
        assert level3_router.prefix == "level1.level2.level3"

    def test_nested_router_project_id_propagation(self):
        level3_router = PubSubRouter()
        level2_router = PubSubRouter(routers=(level3_router,))
        level1_router = PubSubRouter(routers=(level2_router,))
        level1_router._propagate_project_id("test-project")
        assert level2_router.project_id == "test-project"

    def test_nested_router_subscriber_retrieval(self):
        level2_router = PubSubRouter(prefix="level2")

        @level2_router.subscriber(alias="alias", topic_name="topic", subscription_name="sub")
        async def handler():
            pass

        level1_router = PubSubRouter(prefix="level1", routers=(level2_router,))
        subscribers = level1_router._get_subscribers()
        assert "level1.level2.alias" in subscribers
        assert subscribers["level1.level2.alias"].subscription_name == "level1.level2.sub"

    def test_error_on_basic_cyclical_router(self):
        router = PubSubRouter(prefix="")
        with pytest.raises(FastPubSubException):
            router.include_router(router)


class TestPrefixValidation:
    @pytest.mark.parametrize(
        "prefix",
        ["a", "1", "a.b", "a_b", "a/b", "a.b_c/d"],
    )
    def test_valid_prefix(self, prefix):
        # Should not raise
        PubSubRouter(prefix=prefix)

    @pytest.mark.parametrize(
        "prefix",
        [".a", "_a", "/a", "a.", "a_", "a/", "a..b", "a__b", "a//b"],
    )
    def test_invalid_prefix(self, prefix):
        with pytest.raises(FastPubSubException):
            PubSubRouter(prefix=prefix)
