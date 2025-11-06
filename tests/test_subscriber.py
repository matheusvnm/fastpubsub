from typing import Any

import pytest
from pydantic import ValidationError

from fastpubsub.broker import PubSubBroker
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.commands import HandleMessageCommand
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter
from tests.conftest import callstack_matches


def subscriber_create_test_cases():
    default_parameters = {"alias": "alias", "topic_name": "topic", "subscription_name": "sub"}

    return [
        [{"alias": None, "topic_name": "topic", "subscription_name": "sub"}],
        [{"alias": "alias", "topic_name": None, "subscription_name": "sub"}],
        [{"alias": "alias", "topic_name": "topic", "subscription_name": None}],
        [{"alias": "alias", "topic_name": "topic", "subscription_name": 123}],
        [{**default_parameters, "autocreate": None}],
        [{**default_parameters, "autocreate": "yes"}],
        [{**default_parameters, "autocreate": "no"}],
        [{**default_parameters, "autoupdate": None}],
        [{**default_parameters, "autoupdate": "yes"}],
        [{**default_parameters, "autoupdate": "no"}],
        [{**default_parameters, "filter_expression": None}],
        [{**default_parameters, "dead_letter_topic": None}],
        [{**default_parameters, "max_delivery_attempts": None}],
        [{**default_parameters, "ack_deadline_seconds": None}],
        [{**default_parameters, "enable_exactly_once_delivery": None}],
        [{**default_parameters, "min_backoff_delay_secs": None}],
        [{**default_parameters, "max_backoff_delay_secs": None}],
        [{**default_parameters, "max_messages": None}],
        [{**default_parameters, "middlewares": True}],
    ]


@pytest.fixture
def subscriber(broker: PubSubBroker) -> Subscriber:
    async def some_subscriber_handler():
        pass

    broker.subscriber("some_sub", topic_name="some_topic", subscription_name="some_sub_name")(
        some_subscriber_handler
    )
    subscribers = broker.router._get_subscribers()
    return subscribers.popitem()[1]


class TestSubscriber:

    def test_build_callstack(
        self,
        router_a: PubSubRouter,
        router_b: PubSubRouter,
        broker: PubSubBroker,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
    ):
        router_a.include_middleware(first_middleware)
        router_b.include_middleware(second_middleware)
        router_a.include_router(router_b)
        broker.include_router(router_a)

        async def handler_a(_): ...
        async def handler_b(_): ...
        async def handler_broker(_): ...

        router_a.subscriber("sub_a", topic_name="tn", subscription_name="sn")(handler_a)
        router_b.subscriber("sub_b", topic_name="tn", subscription_name="sn")(handler_b)
        broker.subscriber("sub_c", topic_name="tn", subscription_name="sn")(handler_broker)
        subscribers = broker.router._get_subscribers()

        subscriber_a = subscribers["a.sub_a"]
        callstack_a = subscriber_a._build_callstack()
        expected_output = [first_middleware, HandleMessageCommand]
        assert callstack_matches(callstack_a, expected_output)

        subscriber_b = subscribers["a.b.sub_b"]
        callstack_b = subscriber_b._build_callstack()
        expected_output = [second_middleware, first_middleware, HandleMessageCommand]
        assert callstack_matches(callstack_b, expected_output)

        subscriber_c = subscribers["sub_c"]
        callstack_c = subscriber_c._build_callstack()
        expected_output = [HandleMessageCommand]
        assert callstack_matches(callstack_c, expected_output)

    def test_subscriber_name(self, subscriber: Subscriber):
        assert subscriber.name == "some_subscriber_handler"

    def test_subscriber_set_project_id(self, subscriber: Subscriber):
        subscriber._set_project_id("")
        subscriber._set_project_id(None)
        subscriber._set_project_id("some-project")

    def test_subscriber_include_middleware_only_once(
        self,
        subscriber: Subscriber,
        first_middleware: type[BaseMiddleware],
        second_middleware: type[BaseMiddleware],
    ):
        subscriber.include_middleware(first_middleware)
        subscriber.include_middleware(first_middleware)
        subscriber.include_middleware(second_middleware)
        subscriber.include_middleware(second_middleware)
        assert len(subscriber.middlewares) == 2
        assert subscriber.middlewares[0] == first_middleware
        assert subscriber.middlewares[1] == second_middleware

    @pytest.mark.parametrize(
        ["data"],
        subscriber_create_test_cases(),
    )
    def test_subscriber_invalid_data_failed(
        self, data: dict[str, Any], broker: PubSubBroker, router_a: PubSubRouter
    ):
        with pytest.raises(ValidationError):
            broker.subscriber(**data)

        with pytest.raises(ValidationError):
            router_a.subscriber(**data)
