from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from unstructured_ingest.error import ResponseError
from unstructured_ingest.processes.connectors.redisdb import (
    RedisConnectionConfig,
    RedisUploader,
    RedisUploaderConfig,
)

redis_exceptions = pytest.importorskip("redis.exceptions")


def _uploader_raising(mocker, error_message: str):
    pipe = MagicMock()
    pipe.json.return_value.set.return_value.execute = AsyncMock(
        side_effect=redis_exceptions.ResponseError(error_message)
    )
    pipe.set.return_value.execute = AsyncMock()

    client = MagicMock()
    client.pipeline.return_value.__aenter__.return_value = pipe

    @asynccontextmanager
    async def create_async_client(self):
        yield client

    mocker.patch.object(RedisConnectionConfig, "create_async_client", create_async_client)
    uploader = RedisUploader(
        connection_config=RedisConnectionConfig(host="localhost"),
        upload_config=RedisUploaderConfig(),
    )
    return uploader, pipe


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_message",
    [
        # Redis 6
        "unknown command `JSON.SET`, with args beginning with: `a`, `$`, ",
        # Redis 7
        "unknown command 'JSON.SET', with args beginning with: 'a' '$' ",
    ],
)
async def test_check_redis_stack_falls_back_without_json_module(mocker, error_message):
    uploader, pipe = _uploader_raising(mocker, error_message)

    assert await uploader._check_redis_stack({"element_id": "a"}) is False
    pipe.set.assert_called_once_with("a", '{"element_id": "a"}')


@pytest.mark.asyncio
async def test_check_redis_stack_raises_on_other_errors(mocker):
    uploader, pipe = _uploader_raising(mocker, "unknown command 'FOO', with args beginning with: ")

    with pytest.raises(ResponseError):
        await uploader._check_redis_stack({"element_id": "a"})
    pipe.set.assert_not_called()
