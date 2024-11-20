import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional, Union

import pandas as pd
from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    UploadContent,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)

if TYPE_CHECKING:
    from pymilvus import MilvusClient

CONNECTOR_TYPE = "milvus"


class MilvusAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="Milvus password")
    token: Optional[str] = Field(default=None, description="Milvus access token")


class MilvusConnectionConfig(ConnectionConfig):
    access_config: Secret[MilvusAccessConfig] = Field(
        default=MilvusAccessConfig(), validate_default=True
    )
    uri: Optional[str] = Field(
        default=None, description="Milvus uri", examples=["http://localhost:19530"]
    )
    user: Optional[str] = Field(default=None, description="Milvus user")
    db_name: Optional[str] = Field(default=None, description="Milvus database name")

    def get_connection_kwargs(self) -> dict[str, Any]:
        access_config = self.access_config.get_secret_value()
        access_config_dict = access_config.model_dump()
        connection_config_dict = self.model_dump()
        connection_config_dict.pop("access_config", None)
        connection_config_dict.update(access_config_dict)
        # Drop any that were not set explicitly
        connection_config_dict = {k: v for k, v in connection_config_dict.items() if v is not None}
        return connection_config_dict

    @requires_dependencies(["pymilvus"], extras="milvus")
    def get_client(self) -> "MilvusClient":
        from pymilvus import MilvusClient

        return MilvusClient(**self.get_connection_kwargs())


class MilvusUploadStagerConfig(UploadStagerConfig):
    fields_to_include: Optional[list[str]] = None
    """If set - list of fields to include in the output.
    Unspecified fields are removed from the elements.
    This action takes place after metadata flattening.
    Missing fields will cause stager to throw KeyError."""

    flatten_metadata: bool = True
    """If set - flatten "metadata" key and put contents directly into data"""


@dataclass
class MilvusUploadStager(UploadStager):
    upload_stager_config: MilvusUploadStagerConfig = field(
        default_factory=lambda: MilvusUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> float:
        try:
            timestamp = float(date_string)
            return timestamp
        except ValueError:
            pass
        return parser.parse(date_string).timestamp()

    def conform_dict(self, data: dict, file_data: FileData) -> dict:
        working_data = data.copy()
        if self.upload_stager_config.flatten_metadata and (
            metadata := working_data.pop("metadata", None)
        ):
            working_data.update(flatten_dict(metadata, keys_to_omit=["data_source_record_locator"]))

        # TODO: milvus sdk doesn't seem to support defaults via the schema yet,
        #  remove once that gets updated
        defaults = {"is_continuation": False}
        for default in defaults:
            if default not in working_data:
                working_data[default] = defaults[default]

        if self.upload_stager_config.fields_to_include:
            data_keys = set(working_data.keys())
            for data_key in data_keys:
                if data_key not in self.upload_stager_config.fields_to_include:
                    working_data.pop(data_key)
            for field_include_key in self.upload_stager_config.fields_to_include:
                if field_include_key not in working_data:
                    raise KeyError(f"Field '{field_include_key}' is missing in data!")

        datetime_columns = [
            "data_source_date_created",
            "data_source_date_modified",
            "data_source_date_processed",
            "last_modified",
        ]

        json_dumps_fields = ["languages", "data_source_permissions_data"]

        for datetime_column in datetime_columns:
            if datetime_column in working_data:
                working_data[datetime_column] = self.parse_date_string(
                    working_data[datetime_column]
                )
        for json_dumps_field in json_dumps_fields:
            if json_dumps_field in working_data:
                working_data[json_dumps_field] = json.dumps(working_data[json_dumps_field])
        working_data[RECORD_ID_LABEL] = file_data.identifier
        return working_data

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents: list[dict[str, Any]] = json.load(elements_file)
        new_content = [
            self.conform_dict(data=element, file_data=file_data) for element in elements_contents
        ]
        output_filename_path = Path(output_filename)
        if output_filename_path.suffix == ".json":
            output_path = Path(output_dir) / output_filename_path
        else:
            output_path = Path(output_dir) / output_filename_path.with_suffix(".json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w") as output_file:
            json.dump(new_content, output_file, indent=2)
        return output_path


class MilvusUploaderConfig(UploaderConfig):
    db_name: Optional[str] = Field(default=None, description="Milvus database name")
    collection_name: str = Field(description="Milvus collections to write to")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class MilvusUploader(Uploader):
    connection_config: MilvusConnectionConfig
    upload_config: MilvusUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    @DestinationConnectionError.wrap
    def precheck(self):
        with self.get_client() as client:
            if not client.has_collection(self.upload_config.collection_name):
                raise DestinationConnectionError(
                    f"Collection '{self.upload_config.collection_name}' does not exist"
                )

    @contextmanager
    def get_client(self) -> Generator["MilvusClient", None, None]:
        client = self.connection_config.get_client()
        if db_name := self.upload_config.db_name:
            client.using_database(db_name=db_name)
        try:
            yield client
        finally:
            client.close()

    def upload(self, content: UploadContent) -> None:
        file_extension = content.path.suffix
        if file_extension == ".json":
            self.upload_json(content=content)
        elif file_extension == ".csv":
            self.upload_csv(content=content)
        else:
            raise ValueError(f"Unsupported file extension: {file_extension}")

    def delete_by_record_id(self, file_data: FileData) -> None:
        logger.info(
            f"deleting any content with metadata {RECORD_ID_LABEL}={file_data.identifier} "
            f"from milvus collection {self.upload_config.collection_name}"
        )
        with self.get_client() as client:
            delete_filter = f'{self.upload_config.record_id_key} == "{file_data.identifier}"'
            resp = client.delete(
                collection_name=self.upload_config.collection_name, filter=delete_filter
            )
            logger.info(
                "deleted {} records from milvus collection {}".format(
                    resp["delete_count"], self.upload_config.collection_name
                )
            )

    @requires_dependencies(["pymilvus"], extras="milvus")
    def insert_results(self, data: Union[dict, list[dict]]):
        from pymilvus import MilvusException

        logger.info(
            f"uploading {len(data)} entries to {self.connection_config.db_name} "
            f"db in collection {self.upload_config.collection_name}"
        )
        with self.get_client() as client:
            try:
                res = client.insert(collection_name=self.upload_config.collection_name, data=data)
            except MilvusException as milvus_exception:
                raise WriteError("failed to upload records to milvus") from milvus_exception
            if "err_count" in res and isinstance(res["err_count"], int) and res["err_count"] > 0:
                err_count = res["err_count"]
                raise WriteError(f"failed to upload {err_count} docs")

    def upload_csv(self, content: UploadContent) -> None:
        df = pd.read_csv(content.path)
        data = df.to_dict(orient="records")
        self.insert_results(data=data)

    def upload_json(self, content: UploadContent) -> None:
        with content.path.open("r") as file:
            data: list[dict] = json.load(file)
        self.insert_results(data=data)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.delete_by_record_id(file_data=file_data)
        self.upload(content=UploadContent(path=path, file_data=file_data))


milvus_destination_entry = DestinationRegistryEntry(
    connection_config=MilvusConnectionConfig,
    uploader=MilvusUploader,
    uploader_config=MilvusUploaderConfig,
    upload_stager=MilvusUploadStager,
    upload_stager_config=MilvusUploadStagerConfig,
)
