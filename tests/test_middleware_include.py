import pytest

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter


# Include subscriber/publisher middleware

@pytest.fixture
def first_middleware() -> type[BaseMiddleware]:
    class FirstMiddleware(BaseMiddleware): ...
    return FirstMiddleware

@pytest.fixture
def second_middleware() -> type[BaseMiddleware]:
    class SecondMiddleware(BaseMiddleware): ...
    return SecondMiddleware


@pytest.fixture
def third_middleware() -> type[BaseMiddleware]:
    class ThirdMiddleware(BaseMiddleware): ...
    return ThirdMiddleware

@pytest.fixture
def final_middleware() -> type[BaseMiddleware]:
    class FinalMiddleware(BaseMiddleware): ...
    return FinalMiddleware



def test_include_broker_middleware_only_once(first_middleware: type[BaseMiddleware]):
    broker = PubSubBroker(project_id="id", middlewares=(first_middleware, ))
    broker.include_middleware(middleware=first_middleware)
    assert len(broker.router.middlewares) == 1

    other_broker = PubSubBroker(project_id="id2")
    other_broker.include_middleware(middleware=first_middleware)
    assert len(other_broker.router.middlewares) == 1


def test_include_router_middleware_only_once(first_middleware: type[BaseMiddleware]):
    router = PubSubRouter(prefix="core", middlewares=(first_middleware, ))
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


def test_include_middleware_hierarchy(first_middleware: type[BaseMiddleware],
                                      second_middleware: type[BaseMiddleware],
                                      third_middleware: type[BaseMiddleware],):
    child_router = PubSubRouter(prefix="child")
    child_router.include_middleware(third_middleware)

    parent_router = PubSubRouter(prefix="parent")
    parent_router.include_middleware(middleware=second_middleware)
    parent_router.include_router(child_router)

    broker = PubSubBroker(project_id="id")
    broker.include_router(parent_router)
    broker.include_middleware(middleware=first_middleware)


    assert len(child_router.middlewares) == 3
    assert child_router.middlewares[0] == third_middleware
    assert child_router.middlewares[1] == second_middleware
    assert child_router.middlewares[2] == first_middleware

    assert len(parent_router.middlewares) == 2
    assert parent_router.middlewares[0] == second_middleware
    assert parent_router.middlewares[1] == first_middleware

    assert len(broker.router.middlewares) == 1
    assert broker.router.middlewares[0] == first_middleware



def test_include_subscriber_middleware_hierarchy(first_middleware: type[BaseMiddleware],
                                                 second_middleware: type[BaseMiddleware],
                                                 third_middleware: type[BaseMiddleware],
                                                 final_middleware: type[BaseMiddleware]):
    def handle_child(_):
        return _

    def handle_parent(_):
        return _

    def handle_broker(_):
        return _

    child_router = PubSubRouter(prefix="child")
    child_router.include_middleware(third_middleware)
    child_router.subscriber("some_alias",
                topic_name="topic",
                subscription_name="sub",
                middlewares=(final_middleware,))(handle_child)

    parent_router = PubSubRouter(prefix="parent")
    parent_router.include_middleware(middleware=second_middleware)
    parent_router.include_router(child_router)
    parent_router.subscriber("some_alias2",
            topic_name="topic",
            subscription_name="sub2",
            middlewares=(final_middleware,))(handle_parent)


    broker = PubSubBroker(project_id="id")
    broker.include_router(parent_router)
    broker.include_middleware(middleware=first_middleware)
    broker.subscriber("some_alias3",
        topic_name="topic",
        subscription_name="sub3",
        middlewares=(final_middleware,))(handle_broker)


    subscriber: Subscriber = child_router.subscribers.popitem()[1]
    assert len(subscriber.middlewares) == 4
    assert subscriber.middlewares[0] == final_middleware
    assert subscriber.middlewares[1] == third_middleware
    assert subscriber.middlewares[2] == second_middleware
    assert subscriber.middlewares[3] == first_middleware

    subscriber: Subscriber = parent_router.subscribers.popitem()[1]
    assert len(subscriber.middlewares) == 3
    assert subscriber.middlewares[0] == final_middleware
    assert subscriber.middlewares[1] == second_middleware
    assert subscriber.middlewares[2] == first_middleware

    subscriber: Subscriber = broker.router.subscribers.popitem()[1]
    assert len(subscriber.middlewares) == 2
    assert subscriber.middlewares[0] == final_middleware
    assert subscriber.middlewares[1] == first_middleware



def test_include_publisher_middleware_hierarchy(first_middleware: type[BaseMiddleware],
                                                 second_middleware: type[BaseMiddleware],
                                                 third_middleware: type[BaseMiddleware],
                                                 final_middleware: type[BaseMiddleware]):

    child_router = PubSubRouter(prefix="child")
    child_router.include_middleware(third_middleware)

    parent_router = PubSubRouter(prefix="parent")
    parent_router.include_middleware(middleware=second_middleware)
    parent_router.include_router(child_router)

    broker = PubSubBroker(project_id="id")
    broker.include_router(parent_router)
    broker.include_middleware(middleware=first_middleware)

    publisher: Publisher = child_router.publisher(topic_name="some_topic")
    publisher.include_middleware(final_middleware)

    assert len(publisher.middlewares) == 4
    assert publisher.middlewares[0] == third_middleware
    assert publisher.middlewares[1] == second_middleware
    assert publisher.middlewares[2] == first_middleware
    assert publisher.middlewares[3] == final_middleware

    publisher: Publisher = parent_router.publisher(topic_name="another_topic")
    publisher.include_middleware(final_middleware)

    assert len(publisher.middlewares) == 3
    assert publisher.middlewares[0] == second_middleware
    assert publisher.middlewares[1] == first_middleware
    assert publisher.middlewares[2] == final_middleware

    publisher: Publisher = broker.publisher(topic_name="final_ttoic")
    publisher.include_middleware(final_middleware)

    assert len(publisher.middlewares) == 2
    assert publisher.middlewares[0] == first_middleware
    assert publisher.middlewares[1] == final_middleware
