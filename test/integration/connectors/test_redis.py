import asyncio
import json
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pytest
from redis import exceptions as redis_exceptions
from redis.asyncio import Redis, from_url

from test.integration.connectors.utils.constants import DESTINATION_TAG, NOSQL_TAG
from test.integration.utils import requires_env
from unstructured_ingest.v2.interfaces.file_data import FileData, SourceIdentifiers
from unstructured_ingest.v2.processes.connectors.redisdb import (
    CONNECTOR_TYPE as REDIS_CONNECTOR_TYPE,
)
from unstructured_ingest.v2.processes.connectors.redisdb import (
    RedisAccessConfig,
    RedisConnectionConfig,
    RedisUploader,
    RedisUploaderConfig,
)


async def delete_record(client: Redis, element_id: str) -> None:
    await client.delete(element_id)


async def validate_upload(client: Redis, first_element: dict):
    element_id = first_element["element_id"]
    expected_text = first_element["text"]
    expected_embeddings = first_element["embeddings"]
    async with client.pipeline(transaction=True) as pipe:
        try:
            response = await pipe.json().get(element_id, "$").execute()
            response = response[0][0]
        except redis_exceptions.ResponseError:
            response = await pipe.get(element_id).execute()
            response = json.loads(response[0])

    embedding_similarity = np.linalg.norm(
        np.array(response["embeddings"]) - np.array(expected_embeddings)
    )

    assert response is not None
    assert response["element_id"] == element_id
    assert response["text"] == expected_text
    assert embedding_similarity < 1e-10


async def redis_destination_test(
    upload_file: Path,
    tmp_path: Path,
    connection_kwargs: dict,
    uri: Optional[str] = None,
    password: Optional[str] = None,
):
    uploader = RedisUploader(
        connection_config=RedisConnectionConfig(
            **connection_kwargs, access_config=RedisAccessConfig(uri=uri, password=password)
        ),
        upload_config=RedisUploaderConfig(batch_size=10),
    )

    file_data = FileData(
        source_identifiers=SourceIdentifiers(fullpath=upload_file.name, filename=upload_file.name),
        connector_type=REDIS_CONNECTOR_TYPE,
        identifier="mock-file-data",
    )
    with upload_file.open() as upload_fp:
        elements = json.load(upload_fp)
    first_element = elements[0]

    try:
        if uploader.is_async():
            await uploader.run_data_async(data=elements, file_data=file_data)

        if uri:
            async with from_url(uri) as client:
                await validate_upload(client=client, first_element=first_element)
        else:
            async with Redis(**connection_kwargs, password=password) as client:
                await validate_upload(client=client, first_element=first_element)
    except Exception as e:
        raise e
    finally:
        if uri:
            async with from_url(uri) as client:
                tasks = [delete_record(client, element["element_id"]) for element in elements]
                await asyncio.gather(*tasks)
        else:
            async with Redis(**connection_kwargs, password=password) as client:
                tasks = [delete_record(client, element["element_id"]) for element in elements]
                await asyncio.gather(*tasks)


@pytest.mark.asyncio
@pytest.mark.tags(REDIS_CONNECTOR_TYPE, DESTINATION_TAG, NOSQL_TAG)
@requires_env("AZURE_REDIS_INGEST_TEST_PASSWORD")
async def test_redis_destination_azure_with_password(upload_file: Path, tmp_path: Path):
    connection_kwargs = {
        "host": "utic-dashboard-dev.redis.cache.windows.net",
        "port": 6380,
        "db": 0,
        "ssl": True,
    }
    redis_pw = os.environ["AZURE_REDIS_INGEST_TEST_PASSWORD"]
    await redis_destination_test(upload_file, tmp_path, connection_kwargs, password=redis_pw)


@pytest.mark.asyncio
@pytest.mark.tags(REDIS_CONNECTOR_TYPE, DESTINATION_TAG, "redis", NOSQL_TAG)
@requires_env("AZURE_REDIS_INGEST_TEST_PASSWORD")
async def test_redis_destination_azure_with_uri(upload_file: Path, tmp_path: Path):
    connection_kwargs = {}
    redis_pw = os.environ["AZURE_REDIS_INGEST_TEST_PASSWORD"]
    uri = f"rediss://:{redis_pw}@utic-dashboard-dev.redis.cache.windows.net:6380/0"
    await redis_destination_test(upload_file, tmp_path, connection_kwargs, uri=uri)
