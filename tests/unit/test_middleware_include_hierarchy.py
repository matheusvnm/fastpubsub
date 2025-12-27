import pytest
from pydantic import ValidationError

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter


class TestMiddlewareHierarchy:
    @pytest.fixture(autouse=True)
    def broker_with_middlewares(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
        third_middleware: type[BaseMiddleware],
    ):
        router_b.include_middleware(third_middleware)
        router_a.include_middleware(middleware=second_middleware)
        broker.include_middleware(middleware=first_middleware)

    def test_include_broker_middleware_only_once(self, first_middleware: type[BaseMiddleware]):
        new_broker = PubSubBroker(project_id="id", middlewares=(first_middleware,))
        new_broker.include_middleware(middleware=first_middleware)
        assert len(new_broker.router.middlewares) == 1

    def test_include_router_middleware_only_once(self, first_middleware: type[BaseMiddleware]):
        router = PubSubRouter(prefix="core", middlewares=(first_middleware,))
        router.include_middleware(first_middleware)

        broker = PubSubBroker(project_id="id")
        broker.include_router(router)

        assert len(router.middlewares) == 1
        assert len(broker.router.middlewares) == 0

        other_router = PubSubRouter(prefix="core2")
        other_router.include_middleware(first_middleware)

        other_broker = PubSubBroker(project_id="id2")
        other_broker.include_router(other_router)

        assert len(other_router.middlewares) == 1
        assert len(other_broker.router.middlewares) == 0

    def test_nested_router_broker_middleware_hierarchy(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
        third_middleware: type[BaseMiddleware],
    ):
        router_a.include_router(router_b)
        broker.include_router(router_a)

        assert len(router_b.middlewares) == 3
        assert router_b.middlewares[0] == third_middleware
        assert router_b.middlewares[1] == second_middleware
        assert router_b.middlewares[2] == first_middleware

        assert len(router_a.middlewares) == 2
        assert router_a.middlewares[0] == second_middleware
        assert router_a.middlewares[1] == first_middleware

        assert len(broker.router.middlewares) == 1
        assert broker.router.middlewares[0] == first_middleware

    def test_flat_router_broker_middleware_hierarchy(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
        third_middleware: type[BaseMiddleware],
    ):
        broker.include_router(router_a)
        broker.include_router(router_b)

        assert len(router_b.middlewares) == 2
        assert router_b.middlewares[0] == third_middleware
        assert router_b.middlewares[1] == first_middleware

        assert len(router_a.middlewares) == 2
        assert router_a.middlewares[0] == second_middleware
        assert router_a.middlewares[1] == first_middleware

        assert len(broker.router.middlewares) == 1
        assert broker.router.middlewares[0] == first_middleware


class TestSubscriberPublisherMiddlewareHierarchy(TestMiddlewareHierarchy):
    def test_subscriber_middleware_hierarchy(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
        third_middleware: type[BaseMiddleware],
        final_middleware: type[BaseMiddleware],
    ):
        async def handle_child(_):
            return _

        async def handle_parent(_):
            return _

        async def handle_broker(_):
            return _

        router_b.subscriber(
            "some_alias",
            topic_name="topic",
            subscription_name="sub",
            middlewares=(final_middleware,),
        )(handle_child)

        router_a.subscriber(
            "some_alias2",
            topic_name="topic",
            subscription_name="sub2",
            middlewares=(final_middleware,),
        )(handle_parent)

        broker.subscriber(
            "some_alias3",
            topic_name="topic",
            subscription_name="sub3",
            middlewares=(final_middleware,),
        )(handle_broker)

        router_a.include_router(router_b)
        broker.include_router(router_a)

        subscriber: Subscriber = router_b.subscribers.popitem()[1]
        assert len(subscriber.middlewares) == 4
        assert subscriber.middlewares[0] == final_middleware
        assert subscriber.middlewares[1] == third_middleware
        assert subscriber.middlewares[2] == second_middleware
        assert subscriber.middlewares[3] == first_middleware

        subscriber: Subscriber = router_a.subscribers.popitem()[1]
        assert len(subscriber.middlewares) == 3
        assert subscriber.middlewares[0] == final_middleware
        assert subscriber.middlewares[1] == second_middleware
        assert subscriber.middlewares[2] == first_middleware

        subscriber: Subscriber = broker.router.subscribers.popitem()[1]
        assert len(subscriber.middlewares) == 2
        assert subscriber.middlewares[0] == final_middleware
        assert subscriber.middlewares[1] == first_middleware

    def test_publisher_middleware_hierarchy(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
        third_middleware: type[BaseMiddleware],
        final_middleware: type[BaseMiddleware],
    ):
        router_a.include_router(router_b)
        broker.include_router(router_a)

        publisher_b: Publisher = router_b.publisher(topic_name="some_topic")
        publisher_a: Publisher = router_a.publisher(topic_name="another_topic")
        publisher_broker: Publisher = broker.publisher(topic_name="final_ttoic")

        publisher_b.include_middleware(final_middleware)
        publisher_a.include_middleware(final_middleware)
        publisher_broker.include_middleware(final_middleware)

        assert len(publisher_b.middlewares) == 4
        assert publisher_b.middlewares[0] == third_middleware
        assert publisher_b.middlewares[1] == second_middleware
        assert publisher_b.middlewares[2] == first_middleware
        assert publisher_b.middlewares[3] == final_middleware

        assert len(publisher_a.middlewares) == 3
        assert publisher_a.middlewares[0] == second_middleware
        assert publisher_a.middlewares[1] == first_middleware
        assert publisher_a.middlewares[2] == final_middleware

        assert len(publisher_broker.middlewares) == 2
        assert publisher_broker.middlewares[0] == first_middleware
        assert publisher_broker.middlewares[1] == final_middleware

    def test_fail_to_include_non_base_middleware(self):
        class NonBaseMiddleware: ...

        broker = PubSubBroker(project_id="abc")
        with pytest.raises(ValidationError):
            broker.include_middleware(middleware=NonBaseMiddleware)
