import pytest

from docs.snippets.testing.e1_01_setup_asyncio import (
    test_example as asyncio_test,
)


class TestTestingSetupAsyncio:
    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_asyncio_setup_example_runs_successfully(self) -> None:
        # If it runs it works.
        # Wrapped tests always return None
        assert await asyncio_test() is None

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_anyio_setup_example_runs_successfully_under_asyncio(
        self,
    ) -> None:
        # If it runs it works.
        # Wrapped tests always return None
        assert await asyncio_test() is None
