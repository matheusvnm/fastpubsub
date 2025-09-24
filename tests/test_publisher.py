


from pydantic import ValidationError
import pytest
from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from tests.conftest import check_callstack


def test_create_publisher():
    first_router = PubSubRouter(prefix="first")
    first_publisher = first_router.publisher("topic")

    second_router = PubSubRouter(prefix="second")
    second_publisher = second_router.publisher("topic")

    broker = PubSubBroker(project_id="abc")
    broker.include_router(first_router)
    broker.include_router(second_router)

    assert first_publisher.project_id == broker.router.project_id
    assert second_publisher.project_id == broker.router.project_id
    assert first_publisher != second_publisher


    another_first_publisher = first_router.publisher("topic")
    another_second_publisher = second_router.publisher("topic")

    assert first_publisher == another_first_publisher
    assert second_publisher == another_second_publisher


def test_build_callstack(first_middleware: type[BaseMiddleware],
                         second_middleware: type[BaseMiddleware]):

    router_a = PubSubRouter(prefix="a", middlewares=(first_middleware,))
    router_b = PubSubRouter(prefix="b", middlewares=(second_middleware,))
    router_a.include_router(router_b)

    broker = PubSubBroker(project_id="abc")
    broker.include_router(router_a)

    message_publisher_a = router_a.publisher(topic_name="somerandomtopic")
    message_publisher_b = router_b.publisher(topic_name="somerandomtopic")
    message_publisher_c = broker.publisher(topic_name="somerandomtopic")

    callstack_a = message_publisher_a._build_callstack()
    callstack_b = message_publisher_b._build_callstack()
    callstack_c = message_publisher_c._build_callstack()

    expected_output = [first_middleware, PublishMessageCommand]
    check_callstack(callstack_a, expected_output)

    expected_output = [second_middleware, first_middleware, PublishMessageCommand]
    check_callstack(callstack_b, expected_output)

    expected_output = [PublishMessageCommand]
    check_callstack(callstack_c, expected_output)


# Parametrize ()
def test_publisher_invalid_topic_name():
    broker = PubSubBroker(project_id="abc")

    with pytest.raises(ValidationError):
        broker.publisher(topic_name=None)

    with pytest.raises(ValidationError):
        broker.publisher()



def test_publish_invalid_data_fails():
    broker = PubSubBroker(project_id="abc")

    with pytest.raises(ValidationError):
        broker.publish(topic_name=None)

    with pytest.raises(ValidationError):
        broker.publisher()
# Parametrize ()
# Publish
# 1. Invalid topic name (None)
# 2. Invalid topic name (different type (bool/int))
# 3. Invalid serialization type ()
# 4. Invalid ordering key (bool/int)
# 5. Invalid attributes (dict[str, bool], dict[str, int])
# 5. Invalid autocreate (int/str/None)


# Publish # Parametrize ()
# 1. Publish with BaseModel
# 2. Publish with dict[str, Any]
# 3. Publish with str
# 4. Publish with bytes



# Publisher Include Middleware
# 1. Duplicated middleware (Only once)
# 2. Synchronous middleware
# 3. Normal middleware
