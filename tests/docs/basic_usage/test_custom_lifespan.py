import httpx
import pytest

from docs.snippets.basic_usage.e6_02_custom_lifespan import app, global_lifespan


class TestCustomLifespan:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_global_lifespan_creates_and_closes_http_client(self) -> None:
        client_ref: httpx.AsyncClient | None = None

        async with global_lifespan(app):
            client_ref = app.state.http_client
            assert isinstance(client_ref, httpx.AsyncClient)
            assert client_ref.is_closed is False

        assert client_ref is not None
        assert client_ref.is_closed is True
