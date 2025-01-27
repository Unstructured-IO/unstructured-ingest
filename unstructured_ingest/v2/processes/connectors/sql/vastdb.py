from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pandas as pd
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import split_dataframe
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    FileData,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
    _COLUMNS,
    SQLAccessConfig,
    SqlBatchFileData,
    SQLConnectionConfig,
    SQLDownloader,
    SQLDownloaderConfig,
    SQLIndexer,
    SQLIndexerConfig,
    SQLUploader,
    SQLUploaderConfig,
    SQLUploadStager,
    SQLUploadStagerConfig,
)
from unstructured_ingest.v2.utils import get_enhanced_element_id

if TYPE_CHECKING:
    from vastdb import connect as VastdbConnect
    from vastdb import transaction as VastdbTransaction
    from vastdb.table import Table as VastdbTable

CONNECTOR_TYPE = "vastdb"


class VastdbAccessConfig(SQLAccessConfig):
    endpoint: Optional[str] = Field(default=None, description="DB endpoint")
    access_key_id: Optional[str] = Field(default=None, description="access key id")
    access_key_secret: Optional[str] = Field(default=None, description="access key secret")


class VastdbConnectionConfig(SQLConnectionConfig):
    access_config: Secret[VastdbAccessConfig] = Field(
        default=VastdbAccessConfig(), validate_default=True
    )
    vastdb_bucket: str
    vastdb_schema: str
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @requires_dependencies(["vastdb"], extras="vastdb")
    @contextmanager
    def get_connection(self) -> "VastdbConnect":
        from vastdb import connect

        access_config = self.access_config.get_secret_value()
        connection = connect(
            endpoint=access_config.endpoint,
            access=access_config.access_key_id,
            secret=access_config.access_key_secret,
        )
        yield connection

    @contextmanager
    def get_cursor(self) -> "VastdbTransaction":
        with self.get_connection() as connection:
            with connection.transaction() as transaction:
                yield transaction

    @contextmanager
    def get_table(self, table_name: str) -> "VastdbTable":
        with self.get_cursor() as cursor:
            bucket = cursor.bucket(self.vastdb_bucket)
            schema = bucket.schema(self.vastdb_schema)
            table = schema.table(table_name)
            yield table


class VastdbIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class VastdbIndexer(SQLIndexer):
    connection_config: VastdbConnectionConfig
    index_config: VastdbIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _get_doc_ids(self) -> list[str]:
        with self.connection_config.get_table(self.index_config.table_name) as table:
            reader = table.select(columns=[self.index_config.id_column])
            results = reader.read_all()  # Build a PyArrow Table from the RecordBatchReader
            ids = sorted([result[self.index_config.id_column] for result in results.to_pylist()])
            return ids

    def precheck(self) -> None:
        try:
            with self.connection_config.get_table(self.index_config.table_name) as table:
                table.select()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")


class VastdbDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class VastdbDownloader(SQLDownloader):
    connection_config: VastdbConnectionConfig
    download_config: VastdbDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["ibis"], extras="vastdb")
    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        from ibis import _  # imports the Ibis deferred expression

        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = tuple([item.identifier for item in file_data.batch_items])

        with self.connection_config.get_table(table_name) as table:

            predicate = _[id_column].isin(ids)

            if self.download_config.fields:
                # Vastdb requires the id column to be included in the fields
                fields = self.download_config.fields + [id_column]
                # dict.fromkeys to remove duplicates and keep order
                reader = table.select(columns=list(dict.fromkeys(fields)), predicate=predicate)
            else:
                reader = table.select(predicate=predicate)
            results = reader.read_all()
            df = results.to_pandas()
            return [tuple(r) for r in df.to_numpy()], results.column_names


class VastdbUploadStagerConfig(SQLUploadStagerConfig):
    rename_columns_map: Optional[dict] = Field(
        default=None,
        description="Map of column names to rename, ex: {'old_name': 'new_name'}",
    )
    additional_columns: Optional[list[str]] = Field(
        default_factory=list, description="Additional columns to include in the upload"
    )


class VastdbUploadStager(SQLUploadStager):
    upload_stager_config: VastdbUploadStagerConfig

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        data = element_dict.copy()
        metadata: dict[str, Any] = data.pop("metadata", {})
        data_source = metadata.pop("data_source", {})
        coordinates = metadata.pop("coordinates", {})

        data.update(metadata)
        data.update(data_source)
        data.update(coordinates)

        data["id"] = get_enhanced_element_id(element_dict=data, file_data=file_data)

        # remove extraneous, not supported columns
        # but also allow for additional columns
        approved_columns = set(_COLUMNS).union(self.upload_stager_config.additional_columns)
        element = {k: v for k, v in data.items() if k in approved_columns}
        element[RECORD_ID_LABEL] = file_data.identifier
        return element

    def conform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().conform_dataframe(df=df)
        if self.upload_stager_config.rename_columns_map:
            df.rename(columns=self.upload_stager_config.rename_columns_map, inplace=True)
        return df


class VastdbUploaderConfig(SQLUploaderConfig):
    pass


@dataclass
class VastdbUploader(SQLUploader):
    upload_config: VastdbUploaderConfig = field(default_factory=VastdbUploaderConfig)
    connection_config: VastdbConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            with self.connection_config.get_table(self.upload_config.table_name) as table:
                table.select()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["pyarrow"], extras="vastdb")
    def upload_dataframe(self, df: pd.DataFrame, file_data: FileData) -> None:
        import pyarrow as pa

        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        df.replace({np.nan: None}, inplace=True)
        df = self._fit_to_schema(df=df)

        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )

        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):

            with self.connection_config.get_table(self.upload_config.table_name) as table:
                pa_table = pa.Table.from_pandas(rows)
                table.insert(pa_table)

    def get_table_columns(self) -> list[str]:
        if self._columns is None:
            with self.connection_config.get_table(self.upload_config.table_name) as table:
                self._columns = table.columns().names
        return self._columns

    @requires_dependencies(["ibis"], extras="vastdb")
    def delete_by_record_id(self, file_data: FileData) -> None:
        from ibis import _  # imports the Ibis deferred expression

        logger.debug(
            f"deleting any content with data "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from table {self.upload_config.table_name}"
        )
        predicate = _[self.upload_config.record_id_key].isin([file_data.identifier])
        with self.connection_config.get_table(self.upload_config.table_name) as table:
            # Get the internal row id
            rows_to_delete = table.select(
                columns=[], predicate=predicate, internal_row_id=True
            ).read_all()
            table.delete(rows_to_delete)


vastdb_source_entry = SourceRegistryEntry(
    connection_config=VastdbConnectionConfig,
    indexer_config=VastdbIndexerConfig,
    indexer=VastdbIndexer,
    downloader_config=VastdbDownloaderConfig,
    downloader=VastdbDownloader,
)

vastdb_destination_entry = DestinationRegistryEntry(
    connection_config=VastdbConnectionConfig,
    uploader=VastdbUploader,
    uploader_config=VastdbUploaderConfig,
    upload_stager=VastdbUploadStager,
    upload_stager_config=VastdbUploadStagerConfig,
)
