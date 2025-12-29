from unittest.mock import AsyncMock, patch

import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.router import PubSubRouter


class TestCrossProjectSupport:
    def test_router_overrides_broker_project_id(self):
        broker = PubSubBroker(project_id="ProjectA")
        router = PubSubRouter(project_id="ProjectB")
        broker.include_router(router)

        @broker.subscriber("sub_a", topic_name="topic_a", subscription_name="sub_a")
        async def sub_a(msg):
            pass

        @router.subscriber("sub_b", topic_name="topic_b", subscription_name="sub_b")
        async def sub_b(msg):
            pass

        broker_publisher = broker.publisher("topic_a")
        router_publisher = router.publisher("topic_b")

        broker_subscriber = broker.router.subscribers["sub_a"]
        router_subscriber = router.subscribers["sub_b"]

        assert broker_subscriber.project_id == "ProjectA"
        assert router_subscriber.project_id == "ProjectB"

        assert broker_publisher.project_id == "ProjectA"
        assert router_publisher.project_id == "ProjectB"

    def test_pubsub_project_broker_overrides(self):
        broker = PubSubBroker(project_id="ProjectA")

        @broker.subscriber("sub_a", topic_name="topic_a", subscription_name="sub_a")
        async def default_sub_handler(msg):
            pass

        @broker.subscriber(
            "sub_b", topic_name="topic_b", subscription_name="sub_b", project_id="ProjectB"
        )
        async def another_project_sub_handler(msg):
            pass

        default_project_publisher = broker.publisher("topic_a")
        specific_project_publisher = broker.publisher("topic_c", project_id="ProjectC")

        default_project_subscriber = broker.router.subscribers["sub_a"]
        specific_project_subscriber = broker.router.subscribers["sub_b"]

        assert default_project_subscriber.project_id == "ProjectA"
        assert specific_project_subscriber.project_id == "ProjectB"

        assert default_project_publisher.project_id == "ProjectA"
        assert specific_project_publisher.project_id == "ProjectC"

    @pytest.mark.asyncio
    @patch("fastpubsub.router.Publisher")
    async def test_case_broker_publish_override_project_id(self, MockPublisher):
        broker = PubSubBroker(project_id="ProjectA")
        mock_pub_instance = MockPublisher.return_value
        mock_pub_instance.publish = AsyncMock()

        await broker.publish("topic_b", {"data": "test"}, project_id="ProjectB")
        MockPublisher.assert_called_with(
            topic_name="topic_b", project_id="ProjectB", middlewares=[]
        )

        await broker.publish("topic_a", {"data": "test"})
        MockPublisher.assert_called_with(
            topic_name="topic_a", project_id="ProjectA", middlewares=[]
        )

    def test_nested_routers_overrides_project_id_on_composition(self):
        router3 = PubSubRouter(prefix="r3")
        router2 = PubSubRouter(prefix="r2", project_id="ProjectB")
        router1 = PubSubRouter(prefix="r1")
        broker = PubSubBroker(project_id="ProjectA")

        broker.include_router(router1)
        router1.include_router(router2)
        router2.include_router(router3)

        @router1.subscriber("sub", topic_name="t1", subscription_name="s1")
        async def sub1(msg):
            pass

        @router2.subscriber("sub", topic_name="t2", subscription_name="s2")
        async def sub2(msg):
            pass

        @router3.subscriber("sub", topic_name="t3", subscription_name="s3")
        async def sub3(msg):
            pass

        router_1_sub = router1.subscribers["r1.sub"]
        router_2_sub = router2.subscribers["r1.r2.sub"]
        router_3_sub = router3.subscribers["r1.r2.r3.sub"]

        assert router_1_sub.project_id == "ProjectA"
        assert router_2_sub.project_id == "ProjectB"
        assert router_3_sub.project_id == "ProjectB"

    def test_late_binding_router_composition_keeps_project_id(self):
        router = PubSubRouter(project_id="ProjectB")

        @router.subscriber("sub1", topic_name="t1", subscription_name="s1")
        async def sub1(msg):
            pass

        subscriber = router.subscribers["sub1"]
        assert subscriber.project_id == "ProjectB"

        broker = PubSubBroker(project_id="ProjectA")
        broker.include_router(router)

        subscriber = router.subscribers["sub1"]
        assert subscriber.project_id == "ProjectB"
