from unittest.mock import AsyncMock, patch

import pytest

from fastpubsub import PubSubBroker

broker = PubSubBroker(project_id="test-project")


# --8<-- [start:user_service]
class UserService:
    def __init__(self, broker: PubSubBroker):
        self.user_publisher = broker.publisher("user-events")

    async def create_user(self, name: str, email: str):
        user_data = {"name": name, "email": email}
        await self.user_publisher.publish(data=user_data)
        return user_data


# --8<-- [end:user_service]


# --8<-- [start:mock_publisher_test]
@pytest.mark.asyncio
async def test_user_service_publishes_event():
    mock_publisher = AsyncMock()

    with patch.object(broker, "publisher", return_value=mock_publisher):
        service = UserService(broker)
        await service.create_user("Bob", "bob@example.com")

        mock_publisher.publish.assert_called_once()
        call_kwargs = mock_publisher.publish.call_args.kwargs
        assert call_kwargs["data"]["name"] == "Bob"


# --8<-- [end:mock_publisher_test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
