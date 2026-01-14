from fastpubsub.middlewares import Middleware
import json
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.middlewares.di import PublishMessageSerializerMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from tests.conftest import callstack_matches, callstack_to_collection


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

        assert first_publisher != another_first_publisher
        assert second_publisher != another_second_publisher

    def test_build_callstack(
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

        callstack_a = message_publisher_a._build_callstack()
        callstack_b = message_publisher_b._build_callstack()
        callstack_c = message_publisher_c._build_callstack()

        expected_output_a = [first_middleware, PublishMessageSerializerMiddleware]
        assert callstack_matches(callstack_a, expected_output_a)

        expected_output_b = [
            second_middleware,
            first_middleware,
            PublishMessageSerializerMiddleware,
        ]
        assert callstack_matches(callstack_b, expected_output_b)

        expected_output_c = [first_middleware, PublishMessageSerializerMiddleware]
        assert callstack_matches(callstack_c, expected_output_c)


    def test_build_callstack_with_parameters(
        self,
        broker: PubSubBroker,
        router_a: PubSubRouter,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
    ):
        broker.include_middleware(first_middleware, "broker_arg", arg_2="broker_kwarg")
        broker.include_middleware(second_middleware, arg_2="broker_kwargs_only")
        router_a.include_middleware(first_middleware, "router_arg", arg_2="router_kwarg")
        router_a.include_middleware(second_middleware, "router_arg_only")
        broker.include_router(router_a)

        broker_publisher = broker.publisher(topic_name="somerandomtopic")
        broker_callstack = broker_publisher._build_callstack()
        callstack_collection = callstack_to_collection(broker_callstack)
        assert len(callstack_collection) == 3

        first_broker_call = callstack_collection[0]
        assert isinstance(first_broker_call, first_middleware)
        assert first_broker_call.arg_1 == "broker_arg"
        assert first_broker_call.arg_2 == "broker_kwarg"

        second_broker_call = callstack_collection[1]
        assert isinstance(second_broker_call, second_middleware)
        assert second_broker_call.arg_1 == ""
        assert second_broker_call.arg_2 == "broker_kwargs_only"

        router_publisher = router_a.publisher(topic_name="somerandomtopic")
        router_callstack = router_publisher._build_callstack()
        callstack_collection = callstack_to_collection(router_callstack)
        assert len(callstack_collection) == 3

        first_router_call = callstack_collection[0]
        assert isinstance(first_router_call, first_middleware)
        assert first_router_call.arg_1 == "router_arg"
        assert first_router_call.arg_2 == "router_kwarg"

        second_router_call = callstack_collection[1]
        assert isinstance(second_router_call, second_middleware)
        assert second_router_call.arg_1 == "router_arg_only"
        assert second_router_call.arg_2 == ""

    def test_build_callstack_with_parameters_on_constructor(
        self,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
    ):
        router_middlewares = [
            Middleware(first_middleware, "router_arg", arg_2="router_kwarg"),
            Middleware(second_middleware, "router_arg_only"),
        ]

        broker_middlewares = [
            Middleware(first_middleware, "broker_arg", arg_2="broker_kwarg"),
            Middleware(second_middleware, arg_2="broker_kwargs_only"),
        ]

        router = PubSubRouter(middlewares=router_middlewares)
        broker = PubSubBroker("some_project", routers=[router], middlewares=broker_middlewares)

        broker_publisher = broker.publisher(topic_name="somerandomtopic")
        broker_callstack = broker_publisher._build_callstack()
        callstack_collection = callstack_to_collection(broker_callstack)
        assert len(callstack_collection) == 3

        first_broker_call = callstack_collection[0]
        assert isinstance(first_broker_call, first_middleware)
        assert first_broker_call.arg_1 == "broker_arg"
        assert first_broker_call.arg_2 == "broker_kwarg"

        second_broker_call = callstack_collection[1]
        assert isinstance(second_broker_call, second_middleware)
        assert second_broker_call.arg_1 == ""
        assert second_broker_call.arg_2 == "broker_kwargs_only"

        router_publisher = router.publisher(topic_name="somerandomtopic")
        router_callstack = router_publisher._build_callstack()
        callstack_collection = callstack_to_collection(router_callstack)
        assert len(callstack_collection) == 3

        first_router_call = callstack_collection[0]
        assert isinstance(first_router_call, first_middleware)
        assert first_router_call.arg_1 == "router_arg"
        assert first_router_call.arg_2 == "router_kwarg"

        second_router_call = callstack_collection[1]
        assert isinstance(second_router_call, second_middleware)
        assert second_router_call.arg_1 == "router_arg_only"
        assert second_router_call.arg_2 == ""

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
        assert publisher.middlewares[0].cls == first_middleware
        assert publisher.middlewares[1].cls == second_middleware


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
