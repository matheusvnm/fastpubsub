import pytest


@pytest.mark.anyio
async def test_example():
    result = 1 + 1
    assert result == 2
