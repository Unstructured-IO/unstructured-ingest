import os
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.errors_v2 import (
    ProviderError,
    RateLimitError,
    UserAuthError,
    UserError,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class DatabricksPathMixin(BaseModel):
    volume: str = Field(description="Name of volume in the Unity Catalog")
    catalog: str = Field(description="Name of the catalog in the Databricks Unity Catalog service")
    volume_path: Optional[str] = Field(
        default=None, description="Optional path within the volume to write to"
    )
    databricks_schema: str = Field(
        default="default",
        alias="schema",
        description="Schema associated with the volume to write to in the Unity Catalog service",
    )

    @property
    def path(self) -> str:
        path = f"/Volumes/{self.catalog}/{self.databricks_schema}/{self.volume}"
        if self.volume_path:
            path = f"{path}/{self.volume_path}"
        return path


class DatabricksVolumesAccessConfig(AccessConfig):
    token: Optional[str] = Field(default=None, description="Databricks Personal Access Token")


class DatabricksVolumesConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[DatabricksVolumesAccessConfig]
    host: Optional[str] = Field(
        default=None,
        description="The Databricks host URL for either the "
        "Databricks workspace endpoint or the "
        "Databricks accounts endpoint.",
    )

    def wrap_error(self, e: Exception) -> Exception:
        from databricks.sdk.errors.base import DatabricksError
        from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

        if isinstance(e, ValueError):
            error_message = e.args[0]
            message_split = error_message.split(":")
            if message_split[0].endswith("auth"):
                return UserAuthError(e)
        if isinstance(e, DatabricksError):
            reverse_mapping = {v: k for k, v in STATUS_CODE_MAPPING.items()}
            if status_code := reverse_mapping.get(type(e)):
                if status_code in [401, 403]:
                    return UserAuthError(e)
                if status_code == 429:
                    return RateLimitError(e)
                if 400 <= status_code < 500:
                    return UserError(e)
                if 500 <= status_code < 600:
                    return ProviderError(e)
        logger.error(f"unhandled exception from databricks: {e}", exc_info=True)
        return e

    @requires_dependencies(dependencies=["databricks.sdk"], extras="databricks-volumes")
    def get_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config

        config = Config(
            host=self.host,
            **self.access_config.get_secret_value().model_dump(),
        ).with_user_agent_extra(
            "PyDatabricksSdk", os.getenv("UNSTRUCTURED_USER_AGENT", "unstructuredio_oss")
        )

        return WorkspaceClient(config=config)


class DatabricksVolumesIndexerConfig(IndexerConfig, DatabricksPathMixin):
    recursive: bool = False


@dataclass
class DatabricksVolumesIndexer(Indexer, ABC):
    index_config: DatabricksVolumesIndexerConfig
    connection_config: DatabricksVolumesConnectionConfig

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            raise self.connection_config.wrap_error(e=e) from e

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        try:
            for file_info in self.connection_config.get_client().dbfs.list(
                path=self.index_config.path, recursive=self.index_config.recursive
            ):
                if file_info.is_dir:
                    continue
                rel_path = file_info.path.replace(self.index_config.path, "")
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]
                filename = Path(file_info.path).name
                source_identifiers = SourceIdentifiers(
                    filename=filename,
                    rel_path=rel_path,
                    fullpath=file_info.path,
                )
                yield FileData(
                    identifier=str(uuid5(NAMESPACE_DNS, file_info.path)),
                    connector_type=self.connector_type,
                    source_identifiers=source_identifiers,
                    additional_metadata={
                        "catalog": self.index_config.catalog,
                        "path": file_info.path,
                    },
                    metadata=FileDataSourceMetadata(
                        url=file_info.path, date_modified=str(file_info.modification_time)
                    ),
                    display_name=source_identifiers.fullpath,
                )
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)


class DatabricksVolumesDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class DatabricksVolumesDownloader(Downloader, ABC):
    download_config: DatabricksVolumesDownloaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)

    def get_download_path(self, file_data: FileData) -> Path:
        return self.download_config.download_dir / Path(file_data.source_identifiers.relative_path)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        volumes_path = file_data.additional_metadata["path"]
        logger.info(f"Writing {file_data.identifier} to {download_path}")
        try:
            with self.connection_config.get_client().dbfs.download(path=volumes_path) as c:
                read_content = c._read_handle.read()
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)
        with open(download_path, "wb") as f:
            f.write(read_content)
        return self.generate_download_response(file_data=file_data, download_path=download_path)


class DatabricksVolumesUploaderConfig(UploaderConfig, DatabricksPathMixin):
    pass


@dataclass
class DatabricksVolumesUploader(Uploader, ABC):
    upload_config: DatabricksVolumesUploaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    def get_output_path(self, file_data: FileData) -> str:
        if file_data.source_identifiers.relative_path:
            return os.path.join(
                self.upload_config.path,
                f"{file_data.source_identifiers.relative_path.lstrip('/')}.json",
            )
        else:
            return os.path.join(
                self.upload_config.path, f"{file_data.source_identifiers.filename}.json"
            )

    def precheck(self) -> None:
        try:
            assert self.connection_config.get_client().current_user.me().active
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = self.get_output_path(file_data=file_data)
        with open(path, "rb") as elements_file:
            try:
                self.connection_config.get_client().files.upload(
                    file_path=output_path,
                    contents=elements_file,
                    overwrite=True,
                )
            except Exception as e:
                raise self.connection_config.wrap_error(e=e)
