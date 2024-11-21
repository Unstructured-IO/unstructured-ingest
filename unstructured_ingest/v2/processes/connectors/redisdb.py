import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

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
        default=None, description="If not anonymous, use this password, if specified."
    )


class RedisConnectionConfig(ConnectionConfig):
    access_config: Secret[RedisAccessConfig] = Field(
        default=RedisAccessConfig(), validate_default=True
    )
    host: Optional[str] = Field(
        default=None, description="Hostname or IP address of a Redis instance to connect to."
    )
    database: int = Field(default=0, description="Database index to connect to.")
    port: int = Field(default=6379, description="port used to connect to database.")
    username: Optional[str] = Field(
        default=None, description="Username used to connect to database."
    )
    ssl: bool = Field(default=True, description="Whether the connection should use SSL encryption.")
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @requires_dependencies(["redis"], extras="redis")
    @asynccontextmanager
    async def create_client(self) -> AsyncGenerator["Redis", None]:
        from redis.asyncio import Redis, from_url

        access_config = self.access_config.get_secret_value()

        if not access_config.uri and not self.host:
            raise ValueError("Please pass a hostname either directly or through uri")

        options = {
            "host": self.host,
            "port": self.port,
            "db": self.database,
            "ssl": self.ssl,
            "username": self.username,
        }

        if access_config.uri:
            client = from_url(access_config.uri)
        elif access_config.password:
            options["password"] = access_config.password
            client = Redis(**options)
        else:
            client = Redis(**options)

        try:
            yield client
        finally:
            await client.aclose()


class RedisUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")


@dataclass
class RedisUploader(Uploader):
    upload_config: RedisUploaderConfig
    connection_config: RedisConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def precheck(self) -> None:

        async def check_connection():
            async with self.connection_config.create_client() as async_client:
                await async_client.ping()

        try:
            asyncio.run(check_connection())
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["redis"], extras="redis")
    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        from redis import exceptions as redis_exceptions

        with path.open("r") as file:
            elements_dict = json.load(file)

        if not elements_dict:
            return

        logger.info(
            f"writing {len(elements_dict)} objects to destination asynchronously, "
            f"db, {self.connection_config.database}, "
            f"at {self.connection_config.host}",
        )
        async with self.connection_config.create_client() as async_client:
            async with async_client.pipeline(transaction=True) as pipe:
                first_element = elements_dict[0]
                element_id = first_element["element_id"]
                redis_stack = True
                try:
                    # Redis with stack extension supports JSON type
                    await pipe.json().set(element_id, "$", first_element).execute()
                except redis_exceptions.ResponseError as e:
                    message = str(e)
                    if "unknown command `JSON.SET`" in message:
                        # if this error occurs, Redis server doesn't support JSON type,
                        # so save as string type instead
                        await pipe.set(element_id, json.dumps(first_element)).execute()
                        redis_stack = False
                    else:
                        raise e

                for chunk in batch_generator(elements_dict[1:], self.upload_config.batch_size):
                    for element in chunk:
                        element_id = element["element_id"]
                        if redis_stack:
                            pipe.json().set(element_id, "$", element)
                        else:
                            pipe.set(element_id, json.dumps(element))
                    await pipe.execute()


redis_destination_entry = DestinationRegistryEntry(
    connection_config=RedisConnectionConfig,
    uploader=RedisUploader,
    uploader_config=RedisUploaderConfig,
)
