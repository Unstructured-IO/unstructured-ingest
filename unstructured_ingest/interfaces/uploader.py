from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import BaseConnector, BaseProcess
from unstructured_ingest.utils.data_prep import get_json_data


class UploaderConfig(BaseModel):
    pass


UploaderConfigT = TypeVar("UploaderConfigT", bound=UploaderConfig)


@dataclass
class UploadContent:
    path: Path
    file_data: FileData


@dataclass
class Uploader(BaseProcess, BaseConnector, ABC):
    upload_config: UploaderConfigT
    connector_type: str

    def is_async(self) -> bool:
        return False

    def is_batch(self) -> bool:
        return False

    def run_batch(self, contents: list[UploadContent], **kwargs: Any) -> None:
        raise NotImplementedError()

    def create_destination(
        self, destination_name: str = "unstructuredautocreated", **kwargs: Any
    ) -> bool:
        # Update the uploader config if needed with a new destination that gets created.
        # Return a flag on if anything was created or not.
        return False

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data = get_json_data(path=path)
        self.run_data(data=data, file_data=file_data, **kwargs)

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data = get_json_data(path=path)
        await self.run_data_async(data=data, file_data=file_data, **kwargs)

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        return self.run_data(data=data, file_data=file_data, **kwargs)


@dataclass
class VectorDBUploader(Uploader, ABC):
    def create_destination(
        self, vector_length: int, destination_name: str = "unstructuredautocreated", **kwargs: Any
    ) -> bool:
        return False
