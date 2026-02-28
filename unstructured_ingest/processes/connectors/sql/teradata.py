import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.sql.sql import (
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
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import get_enhanced_element_id, split_dataframe
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from pandas import DataFrame
    from teradatasql import TeradataConnection, TeradataCursor

CONNECTOR_TYPE = "teradata"
DEFAULT_TABLE_NAME = "unstructuredautocreated"


def _resolve_db_column_case(
    get_cursor, table_name: str, column_name: str, cache: dict[str, dict[str, str]]
) -> str:
    """Resolve a column name to its actual database case.

    Teradata may store identifiers in uppercase (Enterprise) or lowercase depending
    on how the table was created. Since we double-quote identifiers for reserved-word
    safety, the case must match exactly. This does a one-time SELECT TOP 1 per table
    to discover actual column names from cursor.description.
    """
    if table_name not in cache:
        with get_cursor() as cursor:
            cursor.execute(f'SELECT TOP 1 * FROM "{table_name}"')
            cache[table_name] = {desc[0].lower(): desc[0] for desc in cursor.description}
    return cache[table_name].get(column_name.lower(), column_name)


class TeradataAccessConfig(SQLAccessConfig):
    password: str = Field(description="Teradata user password")


class TeradataConnectionConfig(SQLConnectionConfig):
    access_config: Secret[TeradataAccessConfig]
    host: str = Field(description="Teradata server hostname or IP address")
    user: str = Field(description="Teradata database username")
    database: Optional[str] = Field(
        default=None,
        description="Default database/schema to use for queries",
    )
    dbs_port: int = Field(
        default=1025,
        description="Teradata database port (default: 1025)",
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["teradatasql"], extras="teradata")
    def get_connection(self) -> Generator["TeradataConnection", None, None]:
        from teradatasql import connect

        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.access_config.get_secret_value().password,
            "dbs_port": self.dbs_port,
        }
        if self.database:
            conn_params["database"] = self.database

        connection = connect(**conn_params)
        try:
            yield connection
        finally:
            try:
                connection.commit()
            finally:
                connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["TeradataCursor", None, None]:
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class TeradataIndexerConfig(SQLIndexerConfig):
    pass


@dataclass
class TeradataIndexer(SQLIndexer):
    connection_config: TeradataConnectionConfig
    index_config: TeradataIndexerConfig
    connector_type: str = CONNECTOR_TYPE
    _column_cache: dict = field(init=False, default_factory=dict)

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

        table_name = self.index_config.table_name
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f'SELECT TOP 1 * FROM "{table_name}"')
        except Exception as e:
            logger.error(
                f"Table '{table_name}' not found or not accessible: {e}",
                exc_info=True,
            )
            raise SourceConnectionError(f"Table '{table_name}' not found or not accessible: {e}")

    def _get_doc_ids(self) -> list[str]:
        """Override to quote identifiers for Teradata reserved word handling."""
        id_col = _resolve_db_column_case(
            self.connection_config.get_cursor,
            self.index_config.table_name,
            self.index_config.id_column,
            self._column_cache,
        )
        with self.get_cursor() as cursor:
            cursor.execute(f'SELECT "{id_col}" FROM "{self.index_config.table_name}"')
            results = cursor.fetchall()
            ids = sorted([result[0] for result in results])
            return ids


class TeradataDownloaderConfig(SQLDownloaderConfig):
    pass


@dataclass
class TeradataDownloader(SQLDownloader):
    connection_config: TeradataConnectionConfig
    download_config: TeradataDownloaderConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"
    _column_cache: dict = field(init=False, default_factory=dict)

    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = [item.identifier for item in file_data.batch_items]

        def resolve(col):
            return _resolve_db_column_case(
                self.connection_config.get_cursor,
                table_name,
                col,
                self._column_cache,
            )

        db_id_col = resolve(id_column)

        with self.connection_config.get_cursor() as cursor:
            if self.download_config.fields:
                all_fields = list(dict.fromkeys([id_column] + self.download_config.fields))
                resolved_fields = [resolve(f) for f in all_fields]
                fields = ",".join([f'"{f}"' for f in resolved_fields])
            else:
                fields = "*"

            placeholders = ",".join([self.values_delimiter for _ in ids])
            query = f'SELECT {fields} FROM "{table_name}" WHERE "{db_id_col}" IN ({placeholders})'

            logger.debug(f"running query: {query}\nwith values: {ids}")
            cursor.execute(query, ids)
            rows = cursor.fetchall()
            columns = [col[0].lower() for col in cursor.description]
            return rows, columns



class TeradataUploadStagerConfig(SQLUploadStagerConfig):
    metadata_as_json: bool = Field(
        default=False,
        description="When True, store metadata as a single JSON column instead of "
        "flattening into individual columns. Used with the opinionated "
        "6-column schema.",
    )


@dataclass
class TeradataUploadStager(SQLUploadStager):
    upload_stager_config: TeradataUploadStagerConfig = field(
        default_factory=TeradataUploadStagerConfig
    )

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        if not self.upload_stager_config.metadata_as_json:
            return super().conform_dict(element_dict=element_dict, file_data=file_data)

        data = element_dict.copy()
        return {
            "id": get_enhanced_element_id(element_dict=data, file_data=file_data),
            RECORD_ID_LABEL: file_data.identifier,
            "element_id": data.get("element_id", ""),
            "text": data.get("text"),
            "type": data.get("type"),
            "metadata": json.dumps(data.get("metadata", {})),
        }

    def conform_dataframe(self, df: "DataFrame") -> "DataFrame":
        if self.upload_stager_config.metadata_as_json:
            return df

        df = super().conform_dataframe(df)

        for column in df.columns:
            sample = df[column].dropna().head(10)

            if len(sample) > 0:
                has_complex_type = sample.apply(lambda x: isinstance(x, (list, dict))).any()

                if has_complex_type:
                    df[column] = df[column].apply(
                        lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                    )

        return df


class TeradataUploaderConfig(SQLUploaderConfig):
    table_name: Optional[str] = Field(
        default=None,
        description="Target table name. When None, an opinionated table is "
        "auto-created via create_destination().",
    )


@dataclass
class TeradataUploader(SQLUploader):
    upload_config: TeradataUploaderConfig = field(default_factory=TeradataUploaderConfig)
    connection_config: TeradataConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"

    def init(self, **kwargs: Any) -> None:
        # Auto-creation builds the 6-column JSON blob table, so the stager must have
        # metadata_as_json=True to match. The UI/caller is responsible for setting both.
        self.create_destination(**kwargs)

    def create_destination(
        self, destination_name: str = DEFAULT_TABLE_NAME, **kwargs: Any
    ) -> bool:
        """Create a 6-column opinionated table (id, record_id, element_id, text, type, metadata)
        that stores metadata as a single JSON column instead of flattening into 20+ columns,
        keeping the schema stable as upstream element fields evolve. Requires the stager to
        have metadata_as_json=True so that element metadata is serialized before insert."""
        table_name = self.upload_config.table_name or destination_name
        self.upload_config.table_name = table_name

        with self.get_cursor() as cursor:
            cursor.execute("SELECT DATABASE")
            current_db = cursor.fetchone()[0].strip()
            cursor.execute(
                "SELECT 1 FROM DBC.TablesV "
                "WHERE TableName = ? AND DatabaseName = ? AND TableKind = 'T'",
                [table_name, current_db],
            )
            if cursor.fetchone():
                return False

        connectors_dir = Path(__file__).parents[1]
        schema_file = connectors_dir / "assets" / "teradata_elements_schema.sql"
        with schema_file.open() as f:
            schema_lines = f.readlines()
        schema_lines[0] = schema_lines[0].replace("elements", table_name)
        schema_sql = "".join(line.strip() for line in schema_lines)
        logger.info(f"creating table {table_name} for user")
        with self.get_cursor() as cursor:
            cursor.execute(schema_sql)
        return True

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

        table_name = self.upload_config.table_name
        if table_name:
            try:
                with self.get_cursor() as cursor:
                    cursor.execute(f'SELECT TOP 1 * FROM "{table_name}"')
            except Exception as e:
                logger.error(
                    f"Table '{table_name}' not found or not accessible: {e}",
                    exc_info=True,
                )
                raise DestinationConnectionError(
                    f"Table '{table_name}' not found or not accessible: {e}"
                )

    def get_table_columns(self) -> list[str]:
        if self._columns is None:
            with self.get_cursor() as cursor:
                cursor.execute(f'SELECT TOP 1 * FROM "{self.upload_config.table_name}"')
                self._columns = [desc[0] for desc in cursor.description]
        return self._columns

    def _get_db_column_name(self, name: str) -> str:
        """Resolve a column name to its actual database case via case-insensitive lookup."""
        for col in self.get_table_columns():
            if col.lower() == name.lower():
                return col
        return name

    def delete_by_record_id(self, file_data: FileData) -> None:
        record_id_col = self._get_db_column_name(self.upload_config.record_id_key)
        logger.debug(
            f"deleting any content with data "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from table {self.upload_config.table_name}"
        )
        stmt = (
            f'DELETE FROM "{self.upload_config.table_name}" '
            f'WHERE "{record_id_col}" = {self.values_delimiter}'
        )
        with self.get_cursor() as cursor:
            cursor.execute(stmt, [file_data.identifier])
            rowcount = cursor.rowcount
            if rowcount > 0:
                logger.info(f"deleted {rowcount} rows from table {self.upload_config.table_name}")

    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        import numpy as np

        if self.can_delete():
            self.delete_by_record_id(file_data=file_data)
        else:
            logger.warning(
                f"table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )
        df = self._fit_to_schema(df=df, case_sensitive=False)
        df.replace({np.nan: None}, inplace=True)

        columns = list(df.columns)
        db_col_map = {col.lower(): col for col in self.get_table_columns()}
        db_columns = [db_col_map.get(col.lower(), col) for col in columns]
        quoted_columns = [f'"{col}"' for col in db_columns]

        stmt = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
            table_name=f'"{self.upload_config.table_name}"',
            columns=",".join(quoted_columns),
            values=",".join([self.values_delimiter for _ in columns]),
        )
        logger.info(
            f"writing a total of {len(df)} elements via"
            f" document batches to destination"
            f" table named {self.upload_config.table_name}"
            f" with batch size {self.upload_config.batch_size}"
        )
        for rows in split_dataframe(df=df, chunk_size=self.upload_config.batch_size):
            with self.get_cursor() as cursor:
                values = self.prepare_data(columns, tuple(rows.itertuples(index=False, name=None)))
                logger.debug(f"running query: {stmt}")
                cursor.executemany(stmt, values)


teradata_source_entry = SourceRegistryEntry(
    connection_config=TeradataConnectionConfig,
    indexer_config=TeradataIndexerConfig,
    indexer=TeradataIndexer,
    downloader_config=TeradataDownloaderConfig,
    downloader=TeradataDownloader,
)

teradata_destination_entry = DestinationRegistryEntry(
    connection_config=TeradataConnectionConfig,
    uploader=TeradataUploader,
    uploader_config=TeradataUploaderConfig,
    upload_stager=TeradataUploadStager,
    upload_stager_config=TeradataUploadStagerConfig,
)
