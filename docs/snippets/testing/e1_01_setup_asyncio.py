import pytest


@pytest.mark.asyncio
async def test_example():
    result = 1 + 1
    assert result == 2
