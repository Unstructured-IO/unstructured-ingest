import os
from abc import ABC
from pathlib import Path
from typing import Any, Optional, TypedDict, TypeVar, Union

from pydantic import BaseModel, Field

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces.connector import BaseConnector
from unstructured_ingest.interfaces.process import BaseProcess


class DownloaderConfig(BaseModel):
    download_dir: Optional[Path] = Field(
        default=None,
        description="Where files are downloaded to, defaults to a location at"
        "`$HOME/.cache/unstructured/ingest/<connector name>/<SHA256>`.",
    )


DownloaderConfigT = TypeVar("DownloaderConfigT", bound=DownloaderConfig)


class DownloadResponse(TypedDict):
    file_data: FileData
    path: Path


download_responses = Union[list[DownloadResponse], DownloadResponse]


class Downloader(BaseProcess, BaseConnector, ABC):
    connector_type: str
    download_config: DownloaderConfigT

    def get_download_path(self, file_data: FileData) -> Optional[Path]:
        if not file_data.source_identifiers:
            return None
        
        rel_path = file_data.source_identifiers.relative_path
        if not rel_path:
            return None
        
        rel_path = rel_path[1:] if rel_path.startswith("/") else rel_path
        return self.download_dir / Path(rel_path)

    @staticmethod
    def is_float(value: str):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def generate_download_response(
        self, file_data: FileData, download_path: Path
    ) -> DownloadResponse:
        if (
            file_data.metadata.date_modified
            and self.is_float(file_data.metadata.date_modified)
            and file_data.metadata.date_created
            and self.is_float(file_data.metadata.date_created)
        ):
            date_modified = float(file_data.metadata.date_modified)
            date_created = float(file_data.metadata.date_created)
            os.utime(download_path, times=(date_created, date_modified))
        file_data.local_download_path = str(download_path.resolve())
        return DownloadResponse(file_data=file_data, path=download_path)

    @property
    def download_dir(self) -> Path:
        if self.download_config.download_dir is None:
            self.download_config.download_dir = (
                Path.home()
                / ".cache"
                / "unstructured"
                / "ingest"
                / "download"
                / self.connector_type
            ).resolve()
        return self.download_config.download_dir

    def is_async(self) -> bool:
        return True

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError()

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        return self.run(file_data=file_data, **kwargs)
