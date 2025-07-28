from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.interfaces import BaseConnector, BaseProcess
from unstructured_ingest.processes.utils.logging.connectors import UploaderConnectorLoggingMixin
from unstructured_ingest.utils.data_prep import get_json_data


class UploaderConfig(BaseModel):
    pass


UploaderConfigT = TypeVar("UploaderConfigT", bound=UploaderConfig)


@dataclass
class UploadContent:
    path: Path
    file_data: FileData


@dataclass
class Uploader(BaseProcess, BaseConnector, UploaderConnectorLoggingMixin, ABC):
    upload_config: UploaderConfigT
    connector_type: str

    def __post_init__(self):
        UploaderConnectorLoggingMixin.__init__(self)

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
        self.log_upload_start(path=path, file_data=file_data)
        try:
            self._run(path=path, file_data=file_data, **kwargs)
        except Exception as e:
            self.log_upload_failed(path=path, file_data=file_data, error=e)
            raise e
        self.log_upload_complete(path=path, file_data=file_data)

    def _run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data = get_json_data(path=path)
        self._run_data(data=data, file_data=file_data, **kwargs)

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.log_upload_start(path=path, file_data=file_data)
        try:
            await self._run_async(path=path, file_data=file_data, **kwargs)
        except Exception as e:
            self.log_upload_failed(path=path, file_data=file_data, error=e)
            raise e
        self.log_upload_complete(path=path, file_data=file_data)

    async def _run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data = get_json_data(path=path)
        await self._run_data_async(data=data, file_data=file_data, **kwargs)

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        self.log_upload_start(file_data=file_data)
        try:
            self._run_data(data=data, file_data=file_data, **kwargs)
        except Exception as e:
            self.log_upload_failed(file_data=file_data, error=e)
            raise e
        self.log_upload_complete(file_data=file_data)

    def _run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        raise NotImplementedError()

    async def run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        self.log_upload_start(file_data=file_data)
        try:
            await self._run_data_async(data=data, file_data=file_data, **kwargs)
        except Exception as e:
            self.log_upload_failed(file_data=file_data, error=e)
            raise e
        self.log_upload_complete(file_data=file_data)

    async def _run_data_async(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        self._run_data(data=data, file_data=file_data, **kwargs)

    @property
    # TODO: Convert into @abstractmethod once all existing uploaders have this property
    def endpoint_to_log(self) -> str:
        return ""

    def precheck(self) -> None:
        self.log_connection_validation_start(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log
        )
        try:
            self._precheck()
        except Exception as e:
            self.log_connection_validation_failed(
                connector_type=self.connector_type, error=e, endpoint=self.endpoint_to_log
            )
            raise self.wrap_error(e=e)
        self.log_connection_validation_success(
            connector_type=self.connector_type, endpoint=self.endpoint_to_log
        )

    def _precheck(self) -> None:
        pass

    def wrap_error(self, e: Exception) -> Exception:
        return e


@dataclass
class VectorDBUploader(Uploader, ABC):
    def create_destination(
        self, vector_length: int, destination_name: str = "unstructuredautocreated", **kwargs: Any
    ) -> bool:
        return False
