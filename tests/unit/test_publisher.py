import json
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares import _PublishMessageEncoderMiddleware
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from fastpubsub.serialization import DefaultSerializer
from fastpubsub.serialization.exceptions import EncodingError
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

        # Set serializers for testing
        message_publisher_a.set_serializer(DefaultSerializer())
        message_publisher_b.set_serializer(DefaultSerializer())
        message_publisher_c.set_serializer(DefaultSerializer())

        callstack_a = message_publisher_a._build_callstack()
        callstack_b = message_publisher_b._build_callstack()
        callstack_c = message_publisher_c._build_callstack()

        expected_output_a = [first_middleware, _PublishMessageEncoderMiddleware]
        assert callstack_matches(callstack_a, expected_output_a)

        expected_output_b = [second_middleware, first_middleware, _PublishMessageEncoderMiddleware]
        assert callstack_matches(callstack_b, expected_output_b)

        expected_output_c = [first_middleware, _PublishMessageEncoderMiddleware]
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


class TestDefaultSerializerEncoding:
    """Tests for DefaultSerializer encoding."""

    @pytest.fixture
    def serializer(self) -> DefaultSerializer:
        return DefaultSerializer()

    def test_encode_pydantic_model(self, serializer: DefaultSerializer):
        message = UserSchema(username="Sandro", age=26)
        encoded_data, content_type = serializer.encode(message)
        deserialized_message = json.loads(encoded_data.decode())
        assert message.model_dump() == deserialized_message
        assert content_type == "application/json"

    def test_encode_complex_pydantic_model(self, serializer: DefaultSerializer):
        message = ComplexMessageSchema(
            event_id=uuid4(),
            timestamp=datetime.now(),
            user=UserSchema(username="Test", age=100),
        )
        encoded_data, content_type = serializer.encode(message)
        deserialized_message = json.loads(encoded_data.decode())
        assert deserialized_message["user"]["username"] == "Test"
        assert UUID(deserialized_message["event_id"]) == message.event_id
        assert content_type == "application/json"

    def test_encode_text(self, serializer: DefaultSerializer):
        message = "some_text_string"
        encoded_data, content_type = serializer.encode(message)
        deserialized_message = encoded_data.decode()
        assert message == deserialized_message
        assert content_type == "text/plain"

    def test_encode_dictionary(self, serializer: DefaultSerializer):
        message = {"message": "how are you?"}
        encoded_data, content_type = serializer.encode(message)
        deserialized_message = json.loads(encoded_data.decode())
        assert message == deserialized_message
        assert content_type == "application/json"

    def test_encode_dictionary_with_unserializable_data_raises_exception(
        self, serializer: DefaultSerializer
    ):
        message = {"time": datetime.now()}
        with pytest.raises((EncodingError, TypeError)):
            serializer.encode(message)

    def test_encode_bytes(self, serializer: DefaultSerializer):
        message = b"some_byte_message"
        encoded_data, content_type = serializer.encode(message)
        assert message == encoded_data
        assert content_type == "application/octet-stream"

    def test_encode_integer_as_json(self, serializer: DefaultSerializer):
        # Integers are valid JSON, so they get encoded as JSON
        encoded_data, content_type = serializer.encode(2112)
        assert encoded_data == b"2112"
        assert content_type == "application/json"

    def test_encode_unsupported_type_raises_exception(self, serializer: DefaultSerializer):
        # Objects that can't be JSON serialized should raise EncodingError
        class CustomObject:
            pass

        with pytest.raises(EncodingError):
            serializer.encode(CustomObject())
