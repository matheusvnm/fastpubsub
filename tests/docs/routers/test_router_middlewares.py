import pytest

from docs.snippets.routers.e1_03_router_middlewares import (
    DomainLoggingMiddleware,
    broker,
)
from fastpubsub.testing import PubSubTestClient


class TestRoutersRouterMiddlewares:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_router_middlewares_apply_to_all_router_subscribers(
        self,
    ) -> None:
        async with PubSubTestClient(broker) as client:
            await client.publish(topic="users-topic", data={"user": "alice"})
            await client.publish(
                topic="users-deleted-topic", data={"user": "alice"}
            )
            processed_results = client.get_results()

        assert len(processed_results) == 2

        subscribers = {
            result.message.subscriber_name for result in processed_results
        }
        assert subscribers == {
            "handle_user_created",
            "handle_user_deleted",
        }

        assert all(result.error is None for result in processed_results)

    @pytest.mark.docs
    def test_router_middleware_registration_propagates_into_subscribers(
        self,
    ) -> None:
        subscribers = broker.router._get_subscribers()

        subscriber_created = subscribers["users.created"]
        subscriber_deleted = subscribers["users.deleted"]

        assert len(subscriber_created.middlewares) == 1
        assert len(subscriber_deleted.middlewares) == 1

        assert subscriber_created.middlewares[0].cls is DomainLoggingMiddleware
        assert subscriber_deleted.middlewares[0].cls is DomainLoggingMiddleware
