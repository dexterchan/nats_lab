import pytest
import asyncio

@pytest.mark.asyncio
async def test_some_asyncio_code():
    await asyncio.sleep(10)