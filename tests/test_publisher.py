import json
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel, ValidationError

from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from tests.conftest import check_callstack


class MessageSchema(BaseModel):
    username: str
    age: int


@pytest.fixture
def broker() -> PubSubBroker:
    return PubSubBroker(project_id="abc")


@pytest.fixture
def publisher(broker: PubSubBroker) -> Publisher:
    return broker.publisher(topic_name="cba")


@pytest.mark.asyncio
async def test_publisher_publish_successfully(publisher: Publisher):
    data = b"data_to_send"
    with patch.object(Publisher, "_build_callstack", return_value=AsyncMock()) as mock:
        await publisher.publish(data)
        mock.return_value.on_publish.assert_called_once_with(data, None, None)


def test_create_publisher(broker: PubSubBroker):
    first_router = PubSubRouter(prefix="first")
    first_publisher = first_router.publisher("topic")

    second_router = PubSubRouter(prefix="second")
    second_publisher = second_router.publisher("topic")

    broker.include_router(first_router)
    broker.include_router(second_router)

    assert first_publisher.project_id == broker.router.project_id
    assert second_publisher.project_id == broker.router.project_id
    assert first_publisher != second_publisher

    another_first_publisher = first_router.publisher("topic")
    another_second_publisher = second_router.publisher("topic")

    assert first_publisher == another_first_publisher
    assert second_publisher == another_second_publisher


def test_build_callstack(
    first_middleware: type[BaseMiddleware], second_middleware: type[BaseMiddleware]
):
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


@pytest.mark.parametrize(
    ["topic_name"],
    [
        [None],
        [True],
        [101],
    ],
)
def test_publisher_invalid_topic_name(topic_name: str, broker: PubSubBroker):
    with pytest.raises(ValidationError):
        broker.publisher(topic_name=topic_name)


def test_serialize_pydantic_model(publisher: Publisher):
    message = MessageSchema(username="Sandro", age=26)
    serialized_message = publisher._serialize_message(message)
    deserialized_message = json.loads(serialized_message.decode())

    assert message.model_dump() == deserialized_message


def test_serialize_text(publisher: Publisher):
    message = "some_text_string"
    serialized_message = publisher._serialize_message("some_text_string")
    deserialized_message = serialized_message.decode()

    assert message == deserialized_message


def test_serialize_dictionary(publisher: Publisher):
    message = {"message": "how are you?"}
    serialized_message = publisher._serialize_message(message)
    deserialized_message = json.loads(serialized_message.decode())
    assert message == deserialized_message


def test_serialize_bytes(publisher: Publisher):
    message = b"some_byte_message"
    serialized_message = publisher._serialize_message(message)
    assert message == serialized_message


def test_serialize_invalid_type(publisher: Publisher):
    with pytest.raises(FastPubSubException):
        publisher._serialize_message(2112)


@pytest.mark.parametrize(
    ["data"],
    [
        [{"data": True}],  # Invalid data (bool)
        [{"data": 101, "ordering_key": "key"}],  # Invalid data (int)
        [
            {"data": "text", "ordering_key": {}, "attributes": {"key": "value"}}
        ],  # Invalid ordering key
        [{"data": "text", "attributes": {"key": object}}],  # Invalid attributes,
        [{"data": "text", "autocreate": None}],  # Invalid autocreate
    ],
)
@pytest.mark.asyncio
async def test_publisher_publish_invalid_fields(data: dict[str, Any], publisher: Publisher):
    with pytest.raises(ValidationError):
        await publisher.publish(**data)


def test_publisher_set_project_id(publisher: Publisher):
    publisher.set_project_id("")
    publisher.set_project_id(None)
    publisher.set_project_id("some-project")


def test_publisher_include_middleware_only_once(
    publisher: Publisher,
    first_middleware: type[BaseMiddleware],
    second_middleware: type[BaseMiddleware],
):
    publisher.include_middleware(first_middleware)
    publisher.include_middleware(first_middleware)
    publisher.include_middleware(second_middleware)
    publisher.include_middleware(second_middleware)
    assert len(publisher.middlewares) == 2
    assert publisher.middlewares[0] == first_middleware
    assert publisher.middlewares[1] == second_middleware
