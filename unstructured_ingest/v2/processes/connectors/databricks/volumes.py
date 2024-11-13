import os
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from uuid import NAMESPACE_DNS, uuid5

from pydantic import BaseModel, Field

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger

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


class DatabricksVolumesConnectionConfig(ConnectionConfig, ABC):
    host: Optional[str] = Field(
        default=None,
        description="The Databricks host URL for either the "
        "Databricks workspace endpoint or the "
        "Databricks accounts endpoint.",
    )

    @requires_dependencies(dependencies=["databricks.sdk"], extras="databricks-volumes")
    def get_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient(
            host=self.host,
            **self.access_config.get_secret_value().model_dump(),
        )


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
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for file_info in self.connection_config.get_client().dbfs.list(
            path=self.index_config.path, recursive=self.index_config.recursive
        ):
            if file_info.is_dir:
                continue
            rel_path = file_info.path.replace(self.index_config.path, "")
            if rel_path.startswith("/"):
                rel_path = rel_path[1:]
            filename = Path(file_info.path).name
            yield FileData(
                identifier=str(uuid5(NAMESPACE_DNS, file_info.path)),
                connector_type=self.connector_type,
                source_identifiers=SourceIdentifiers(
                    filename=filename,
                    rel_path=rel_path,
                    fullpath=file_info.path,
                ),
                additional_metadata={"catalog": self.index_config.catalog, "path": file_info.path},
                metadata=FileDataSourceMetadata(
                    url=file_info.path, date_modified=str(file_info.modification_time)
                ),
            )


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
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

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
            with open(download_path, "wb") as f:
                f.write(read_content)
        except Exception as e:
            logger.error(f"failed to download file {file_data.identifier}: {e}", exc_info=True)
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")

        return self.generate_download_response(file_data=file_data, download_path=download_path)


class DatabricksVolumesUploaderConfig(UploaderConfig, DatabricksPathMixin):
    pass


@dataclass
class DatabricksVolumesUploader(Uploader, ABC):
    upload_config: DatabricksVolumesUploaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    def precheck(self) -> None:
        try:
            assert self.connection_config.get_client().current_user.me().active
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = os.path.join(
            self.upload_config.path, f"{file_data.source_identifiers.filename}.json"
        )
        with open(path, "rb") as elements_file:
            self.connection_config.get_client().files.upload(
                file_path=output_path,
                contents=elements_file,
                overwrite=True,
            )
