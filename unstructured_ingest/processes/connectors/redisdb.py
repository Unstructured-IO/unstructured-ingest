import json
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncGenerator, Generator, Optional

from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import DestinationRegistryEntry
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from redis.asyncio import Redis

import asyncio

CONNECTOR_TYPE = "redis"
SERVER_API_VERSION = "1"


class RedisAccessConfig(AccessConfig):
    uri: Optional[str] = Field(
        default=None, description="If not anonymous, use this uri, if specified."
    )
    password: Optional[str] = Field(
        default=None,
        description="Password used to connect to database if uri is "
        "not specified and connection is not anonymous.",
    )


class RedisConnectionConfig(ConnectionConfig):
    access_config: Secret[RedisAccessConfig] = Field(
        default=RedisAccessConfig(), validate_default=True
    )
    host: Optional[str] = Field(
        default=None,
        description="Hostname or IP address of a Redis instance to connect to "
        "if uri is not specified.",
    )
    database: int = Field(default=0, description="Database index to connect to.")
    port: Optional[int] = Field(
        default=6379, description="Port used to connect to database if uri is not specified."
    )
    username: Optional[str] = Field(
        default=None, description="Username used to connect to database if uri is not specified."
    )
    ssl: Optional[bool] = Field(
        default=True,
        description="Whether the connection should use SSL encryption if uri is not specified.",
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @model_validator(mode="after")
    def validate_host_or_url(self) -> "RedisConnectionConfig":
        if not self.access_config.get_secret_value().uri:
            if not self.host:
                raise ValueError("Please pass a hostname either directly or through uri")
            if self.port is None:
                raise ValueError("Since URI is not specified, port cannot be None")
            if self.ssl is None:
                raise ValueError("Since URI is not specified, ssl cannot be None")
        return self

    @requires_dependencies(["redis"], extras="redis")
    @asynccontextmanager
    async def create_async_client(self) -> AsyncGenerator["Redis", None]:
        from redis.asyncio import Redis, from_url

        access_config = self.access_config.get_secret_value()

        if access_config.uri:
            async with from_url(access_config.uri) as client:
                yield client
        else:
            options = {
                "host": self.host,
                "port": self.port,
                "db": self.database,
                "ssl": self.ssl,
                "username": self.username,
            }

            if access_config.password:
                options["password"] = access_config.password
            async with Redis(**options) as client:
                yield client

    @requires_dependencies(["redis"], extras="redis")
    @contextmanager
    def create_client(self) -> Generator["Redis", None, None]:
        from redis import Redis, from_url

        access_config = self.access_config.get_secret_value()

        options = {
            "host": self.host,
            "port": self.port,
            "db": self.database,
            "ssl": self.ssl,
            "username": self.username,
        }

        if access_config.password:
            options["password"] = access_config.password

        if access_config.uri:
            with from_url(access_config.uri) as client:
                yield client
        else:
            with Redis(**options) as client:
                yield client


class RedisUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")
    key_prefix: str = Field(default="", description="Prefix for Redis keys")


def _form_redis_pipeline_error_message(error: str) -> str:
    """
    Form a user-friendly error message for Redis pipeline errors.
    The error message has `$` character at the beginning and `) of pipeline` at the end.
    Everything between these two strings is the value an should be removed.
    """
    start = error.find("$")
    end = error.find(") of pipeline")
    if start != -1 and end != -1:
        return error[: start + 1] + "<value>" + error[end:]
    else:
        return error


@dataclass
class RedisUploader(Uploader):
    upload_config: RedisUploaderConfig
    connection_config: RedisConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def precheck(self) -> None:
        try:
            with self.connection_config.create_client() as client:
                client.ping()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        first_element = data[0]
        redis_stack = await self._check_redis_stack(first_element)
        logger.info(
            f"writing {len(data)} objects to destination asynchronously, "
            f"db, {self.connection_config.database}, "
            f"at {self.connection_config.host}",
        )

        batches = list(batch_generator(data, batch_size=self.upload_config.batch_size))
        await asyncio.gather(*[self._write_batch(batch, redis_stack) for batch in batches])

    async def _write_batch(self, batch: list[dict], redis_stack: bool) -> None:
        async with (
            self.connection_config.create_async_client() as async_client,
            async_client.pipeline(transaction=True) as pipe,
        ):
            for element in batch:
                key_with_prefix = f"{self.upload_config.key_prefix}{element['element_id']}"
                if redis_stack:
                    pipe.json().set(key_with_prefix, "$", element)
                else:
                    pipe.set(key_with_prefix, json.dumps(element))
            await pipe.execute()

    @requires_dependencies(["redis"], extras="redis")
    async def _check_redis_stack(self, element: dict) -> bool:
        from redis import exceptions as redis_exceptions

        redis_stack = True
        async with (
            self.connection_config.create_async_client() as async_client,
            async_client.pipeline(transaction=True) as pipe,
        ):
            key_with_prefix = f"{self.upload_config.key_prefix}{element['element_id']}"
            try:
                # Redis with stack extension supports JSON type
                await pipe.json().set(key_with_prefix, "$", element).execute()
            except redis_exceptions.ResponseError as e:
                message = _form_redis_pipeline_error_message(str(e))
                if "unknown command `JSON.SET`" in message:
                    # if this error occurs, Redis server doesn't support JSON type,
                    # so save as string type instead
                    await pipe.set(key_with_prefix, json.dumps(element)).execute()
                    redis_stack = False
                else:
                    raise redis_exceptions.ResponseError(message) from e
        return redis_stack


redis_destination_entry = DestinationRegistryEntry(
    connection_config=RedisConnectionConfig,
    uploader=RedisUploader,
    uploader_config=RedisUploaderConfig,
)
