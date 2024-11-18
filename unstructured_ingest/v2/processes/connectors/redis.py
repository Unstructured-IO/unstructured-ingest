import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret
from redis import exceptions as redis_exceptions

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from redis.asyncio import Redis

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
        default=None, description="hostname or IP address of a single redis instance to connect to"
    )
    database: Optional[int] = Field(default=0, description="database index to connect to")
    port: Optional[int] = Field(default=6379)
    username: Optional[str] = Field(default=None)
    ssl: Optional[bool] = Field(default=True)
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)


class RedisUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class RedisUploadStager(UploadStager):
    upload_stager_config: RedisUploadStagerConfig = field(
        default_factory=lambda: RedisUploadStagerConfig()
    )

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        shutil.copy(elements_filepath, output_path)
        return output_path


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
        if not self.connection_config.access_config.uri and not self.connection_config.host:
            raise ValueError("Please pass a hostname either directly or through uri")

        try:
            client = self.create_client()
            client.ping()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["redis"])
    def create_client(self, async_flag=Field(default=True)) -> "Redis":
        if async_flag:
            from redis.asyncio import Redis, from_url
        else:
            from redis import Redis, from_url

        access_config = self.connection_config.access_config.get_secret_value()

        if access_config.uri:
            return from_url(access_config.uri)
        elif access_config.password:
            return Redis(
                host=self.connection_config.host,
                port=self.connection_config.port,
                db=self.connection_config.database,
                password=access_config.password,
                ssl=self.connection_config.ssl,
            )
        else:
            return Redis(
                host=self.connection_config.host,
                port=self.connection_config.port,
                db=self.connection_config.database,
                ssl=self.connection_config.ssl,
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:

        with path.open("r") as file:
            elements_dict = json.load(file)

        if elements_dict:
            logger.info(
                f"writing {len(elements_dict)} objects to destination, "
                f"db, {self.connection_config.database}, "
                f"at {self.connection_config.host}",
            )
            client = self.create_client(async_flag=False)
            pipeline = client.pipeline()
            first_element = elements_dict[0]
            element_id = first_element["element_id"]
            redis_stack = True
            try:
                pipeline.json().set(element_id, "$", first_element)
                pipeline.execute()
            except redis_exceptions.ResponseError:
                # if redis server doesn't have stack extension,
                # then cannot save as JSON type, save as string instead
                pipeline.set(element_id, json.dumps(first_element))
                pipeline.execute()
                redis_stack = False

            for chunk in batch_generator(elements_dict[1:], self.upload_config.batch_size):
                for element in chunk:
                    element_id = element["element_id"]
                    if redis_stack:
                        pipeline.json().set(element_id, "$", element)
                    else:
                        pipeline.set(element_id, json.dumps(element))
                pipeline.execute()
            client.close()

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)

        if elements_dict:
            logger.info(
                f"writing {len(elements_dict)} objects to destination asynchronously, "
                f"db, {self.connection_config.database}, "
                f"at {self.connection_config.host}",
            )
            client = await self.create_client(async_flag=True)
            async with client.pipeline(transaction=True) as pipe:
                first_element = elements_dict[0]
                element_id = first_element["element_id"]
                redis_stack = True
                try:
                    await pipe.json().set(element_id, "$", first_element).execute()
                except redis_exceptions.ResponseError:
                    # if redis server doesn't have stack extension,
                    # then cannot save as JSON type, save as string instead
                    await pipe.set(element_id, json.dumps(first_element)).execute()
                    redis_stack = False

                for chunk in batch_generator(elements_dict[1:], self.upload_config.batch_size):
                    for element in chunk:
                        element_id = element["element_id"]
                        if redis_stack:
                            pipe.json().set(element_id, "$", element)
                        else:
                            pipe.set(element_id, json.dumps(element))
                    await pipe.execute()
            await client.aclose()


redis_destination_entry = DestinationRegistryEntry(
    connection_config=RedisConnectionConfig,
    uploader=RedisUploader,
    uploader_config=RedisUploaderConfig,
    upload_stager=RedisUploadStager,
    upload_stager_config=RedisUploadStagerConfig,
)
