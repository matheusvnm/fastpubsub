import json
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import PublishMessageCommand
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from tests.conftest import callstack_matches


class UserSchema(BaseModel):
    username: str
    age: int


class ComplexMessageSchema(BaseModel):
    event_id: UUID
    timestamp: datetime
    user: UserSchema


class TestPublisher:
    def test_create_publisher_instances(
        self, router_a: PubSubRouter, router_b: PubSubRouter, broker: PubSubBroker
    ):
        first_publisher = router_a.publisher("topic")
        second_publisher = router_b.publisher("topic")

        broker.include_router(router_a)
        broker.include_router(router_b)

        assert first_publisher.project_id == broker.router.project_id
        assert second_publisher.project_id == broker.router.project_id
        assert first_publisher != second_publisher

        another_first_publisher = router_a.publisher("topic")
        another_second_publisher = router_b.publisher("topic")

        assert first_publisher == another_first_publisher
        assert second_publisher == another_second_publisher

    @pytest.mark.asyncio
    async def test_build_callstack(
        self,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        broker: PubSubBroker,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
    ):
        broker.include_middleware(first_middleware)
        router_a.include_middleware(first_middleware)
        router_b.include_middleware(second_middleware)
        router_a.include_router(router_b)
        broker.include_router(router_a)

        message_publisher_a = router_a.publisher(topic_name="somerandomtopic")
        message_publisher_b = router_b.publisher(topic_name="somerandomtopic")
        message_publisher_c = broker.publisher(topic_name="somerandomtopic")

        callstack_a = await message_publisher_a._build_callstack()
        callstack_b = await message_publisher_b._build_callstack()
        callstack_c = await message_publisher_c._build_callstack()

        expected_output_a = [first_middleware, PublishMessageCommand]
        assert callstack_matches(callstack_a, expected_output_a)

        expected_output_b = [second_middleware, first_middleware, PublishMessageCommand]
        assert callstack_matches(callstack_b, expected_output_b)

        expected_output_c = [first_middleware, PublishMessageCommand]
        assert callstack_matches(callstack_c, expected_output_c)

    @pytest.mark.parametrize(
        "project_id",
        [
            [""],
            [None],
            ["some-project"],
        ],
    )
    def test_set_project_id(self, publisher: Publisher, project_id: str):
        publisher._set_project_id(project_id)

    def test_include_middleware_only_once(
        self,
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


class TestPublisherSerialization:
    @pytest.mark.asyncio
    async def test_serialize_pydantic_model(self, publisher: Publisher):
        message = UserSchema(username="Sandro", age=26)
        serialized_message = await publisher._serialize_message(message)
        deserialized_message = json.loads(serialized_message.decode())
        assert message.model_dump() == deserialized_message

    @pytest.mark.asyncio
    async def test_serialize_complex_pydantic_model(self, publisher: Publisher):
        message = ComplexMessageSchema(
            event_id=uuid4(),
            timestamp=datetime.now(),
            user=UserSchema(username="Test", age=100),
        )
        serialized_message = await publisher._serialize_message(message)
        deserialized_message = json.loads(serialized_message.decode())
        assert deserialized_message["user"]["username"] == "Test"
        assert UUID(deserialized_message["event_id"]) == message.event_id

    @pytest.mark.asyncio
    async def test_serialize_text(self, publisher: Publisher):
        message = "some_text_string"
        serialized_message = await publisher._serialize_message("some_text_string")
        deserialized_message = serialized_message.decode()
        assert message == deserialized_message

    @pytest.mark.asyncio
    async def test_serialize_dictionary(self, publisher: Publisher):
        message = {"message": "how are you?"}
        serialized_message = await publisher._serialize_message(message)
        deserialized_message = json.loads(serialized_message.decode())
        assert message == deserialized_message

    @pytest.mark.asyncio
    async def test_serialize_dictionary_with_unserializable_data_raises_exception(
        self, publisher: Publisher
    ):
        message = {"time": datetime.now()}
        with pytest.raises(TypeError):
            await publisher._serialize_message(message)

    @pytest.mark.asyncio
    async def test_serialize_bytes(self, publisher: Publisher):
        message = b"some_byte_message"
        serialized_message = await publisher._serialize_message(message)
        assert message == serialized_message

    @pytest.mark.asyncio
    async def test_serialize_invalid_type_raises_exception(self, publisher: Publisher):
        with pytest.raises(FastPubSubException):
            await publisher._serialize_message(2112)
