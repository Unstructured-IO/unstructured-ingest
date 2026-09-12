import hashlib
import json
from abc import ABC, abstractmethod
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional, Union

from dateutil import parser
from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    BatchFileData,
    BatchItem,
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    UnstructuredIngestError,
    UserError,
    safe_error_summary,
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
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import (
    get_data_df,
    get_enhanced_element_id,
    get_json_data,
    split_dataframe,
    write_data,
)

if TYPE_CHECKING:
    from pandas import DataFrame

_DATE_COLUMNS = ("date_created", "date_modified", "date_processed", "last_modified")


class SqlAdditionalMetadata(BaseModel):
    table_name: str
    id_column: str


class SqlBatchFileData(BatchFileData):
    additional_metadata: SqlAdditionalMetadata


def parse_date_string(date_value: Union[str, int]) -> datetime:
    try:
        timestamp = float(date_value) / 1000 if isinstance(date_value, int) else float(date_value)
        return datetime.fromtimestamp(timestamp)
    except Exception as e:
        logger.debug(f"date {date_value} string not a timestamp: {e}")

    if isinstance(date_value, str):
        try:
            return datetime.fromisoformat(date_value)
        except Exception:
            pass
    return parser.parse(date_value)


class SQLAccessConfig(AccessConfig):
    pass


class SQLConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[SQLAccessConfig] = Field(default=SQLAccessConfig(), validate_default=True)

    @abstractmethod
    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        pass

    @abstractmethod
    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        pass


class SQLIndexerConfig(IndexerConfig):
    table_name: str
    id_column: str
    batch_size: int = 100


class SQLIndexer(Indexer, ABC):
    connection_config: SQLConnectionConfig
    index_config: SQLIndexerConfig

    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        with self.connection_config.get_cursor() as cursor:
            yield cursor

    def _get_doc_ids(self) -> list[str]:
        with self.get_cursor() as cursor:
            cursor.execute(
                f"SELECT {self.index_config.id_column} FROM {self.index_config.table_name}"
            )
            results = cursor.fetchall()
            ids = sorted([result[0] for result in results])
            return ids

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1;")
        except (ImportError, UnstructuredIngestError):
            # Preserve dependency-install guidance and connector-authored typed
            # errors; only unexpected exceptions are redacted below.
            raise
        except Exception as e:
            logger.error(f"failed to validate connection: {safe_error_summary(e)}")
            raise SourceConnectionError(
                f"failed to validate connection: {safe_error_summary(e)}"
            ) from None

    def run(self, **kwargs: Any) -> Generator[SqlBatchFileData, None, None]:
        ids = self._get_doc_ids()
        id_batches: list[frozenset[str]] = [
            frozenset(
                ids[
                    i * self.index_config.batch_size : (i + 1)  # noqa
                    * self.index_config.batch_size
                ]
            )
            for i in range(
                (len(ids) + self.index_config.batch_size - 1) // self.index_config.batch_size
            )
        ]

        for batch in id_batches:
            batch_items = [BatchItem(identifier=str(b)) for b in batch]
            display_name = (
                f"{self.index_config.table_name}-{self.index_config.id_column}"
                f"-[{batch_items[0].identifier}..{batch_items[-1].identifier}]"
            )
            # Make sure the hash is always a positive number to create identified
            yield SqlBatchFileData(
                connector_type=self.connector_type,
                metadata=FileDataSourceMetadata(
                    date_processed=str(time()),
                ),
                additional_metadata=SqlAdditionalMetadata(
                    table_name=self.index_config.table_name, id_column=self.index_config.id_column
                ),
                batch_items=batch_items,
                display_name=display_name,
            )


class SQLDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


class SQLDownloader(Downloader, ABC):
    connection_config: SQLConnectionConfig
    download_config: SQLDownloaderConfig

    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        with self.connection_config.get_cursor() as cursor:
            yield cursor

    @abstractmethod
    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        pass

    def sql_to_df(self, rows: list[tuple], columns: list[str]) -> list["DataFrame"]:
        import pandas as pd

        data = [dict(zip(columns, row)) for row in rows]
        df = pd.DataFrame(data)
        dfs = [pd.DataFrame([row.values], columns=df.columns) for index, row in df.iterrows()]
        return dfs

    def get_data(self, file_data: SqlBatchFileData) -> list["DataFrame"]:
        rows, columns = self.query_db(file_data=file_data)
        return self.sql_to_df(rows=rows, columns=columns)

    def get_identifier(self, table_name: str, record_id: str) -> str:
        f = f"{table_name}-{record_id}"
        if self.download_config.fields:
            f = "{}-{}".format(
                f,
                hashlib.sha256(",".join(self.download_config.fields).encode()).hexdigest()[:8],
            )
        return f

    @staticmethod
    def _resolve_column_name(df: "DataFrame", column_name: str) -> str:
        if column_name in df.columns:
            return column_name
        columns_lower = {col.lower(): col for col in df.columns}
        if column_name.lower() in columns_lower:
            return columns_lower[column_name.lower()]
        return column_name

    def generate_download_response(
        self, result: "DataFrame", file_data: SqlBatchFileData
    ) -> DownloadResponse:
        id_column = file_data.additional_metadata.id_column
        table_name = file_data.additional_metadata.table_name
        resolved_id_column = self._resolve_column_name(result, id_column)
        record_id = result.iloc[0][resolved_id_column]
        filename_id = self.get_identifier(table_name=table_name, record_id=record_id)
        filename = f"{filename_id}.csv"
        download_path = self.download_dir / Path(filename)
        logger.debug(
            f"Downloading results from table {table_name} and id {record_id} to {download_path}"
        )
        download_path.parent.mkdir(parents=True, exist_ok=True)
        result.to_csv(download_path, index=False)
        file_data.source_identifiers = SourceIdentifiers(
            filename=filename,
            fullpath=filename,
        )
        cast_file_data = FileData.cast(file_data=file_data)
        cast_file_data.identifier = filename_id
        return super().generate_download_response(
            file_data=cast_file_data, download_path=download_path
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        sql_filedata = SqlBatchFileData.cast(file_data=file_data)
        data_dfs = self.get_data(file_data=sql_filedata)
        download_responses = []
        for df in data_dfs:
            download_responses.append(
                self.generate_download_response(result=df, file_data=sql_filedata)
            )
        return download_responses


class SQLUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class SQLUploadStager(UploadStager):
    upload_stager_config: SQLUploadStagerConfig = field(default_factory=SQLUploadStagerConfig)

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        data = element_dict.copy()
        metadata: dict[str, Any] = data.pop("metadata", {})
        data_source = metadata.pop("data_source", {})
        coordinates = metadata.pop("coordinates", {})

        data.update(metadata)
        data.update(data_source)
        data.update(coordinates)

        data["id"] = get_enhanced_element_id(element_dict=data, file_data=file_data)

        data[RECORD_ID_LABEL] = file_data.identifier
        return data

    def conform_dataframe(self, df: "DataFrame") -> "DataFrame":
        for column in filter(lambda x: x in df.columns, _DATE_COLUMNS):
            df[column] = df[column].apply(parse_date_string).apply(lambda date: date.timestamp())
        for column in filter(
            lambda x: x in df.columns,
            ("permissions_data", "record_locator", "points", "links"),
        ):
            df[column] = df[column].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None
            )
        for column in filter(
            lambda x: x in df.columns,
            ("version", "page_number", "regex_metadata"),
        ):
            df[column] = df[column].apply(str)
        for column in df.columns:
            if df[column].apply(lambda x: isinstance(x, dict)).any():
                df[column] = df[column].apply(
                    lambda x: json.dumps(x) if isinstance(x, dict) else x
                )
        return df

    def write_output(self, output_path: Path, data: list[dict]) -> Path:
        write_data(path=output_path, data=data)
        return output_path

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        import pandas as pd

        elements_contents = get_json_data(path=elements_filepath)

        df = pd.DataFrame(
            data=[
                self.conform_dict(element_dict=element_dict, file_data=file_data)
                for element_dict in elements_contents
            ]
        )
        df = self.conform_dataframe(df=df)

        output_filename_suffix = Path(elements_filepath).suffix
        output_filename = f"{Path(output_filename).stem}{output_filename_suffix}"
        output_path = self.get_output_path(output_filename=output_filename, output_dir=output_dir)

        final_output_path = self.write_output(
            output_path=output_path, data=df.to_dict(orient="records")
        )
        return final_output_path


class SQLUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of records per batch")
    table_name: str = Field(
        default="elements",
        description="which table to upload contents to",
        json_schema_extra={"x-runtime-eligible": True},
    )
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class SQLUploader(Uploader):
    upload_config: SQLUploaderConfig
    connection_config: SQLConnectionConfig
    values_delimiter: str = "?"
    _columns: list[str] = field(init=False, default=None)

    def precheck(self) -> None:
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1;")
        except (ImportError, UnstructuredIngestError):
            # Preserve dependency-install guidance and connector-authored typed
            # errors; only unexpected exceptions are redacted below.
            raise
        except Exception as e:
            logger.error(f"failed to validate connection: {safe_error_summary(e)}")
            raise DestinationConnectionError(
                f"failed to validate connection: {safe_error_summary(e)}"
            ) from None
        # Outside the block above on purpose: a connection failure must keep its
        # existing DestinationConnectionError, and the probe raises only UserError.
        self.check_write_permissions()

    def check_write_permissions(self) -> None:
        """Refuse a destination credential the database says cannot perform the write.

        ``SELECT 1`` above runs against the session, so it succeeds on any credential
        the driver can open a connection with, whatever rights that credential holds on
        the table this uploader writes to. A credential that can connect and read
        therefore passes the connection check and then fails on every record at write
        time, which is the failure this asks about up front instead.

        The probe's privilege surface has to match the upload's exactly -- no wider, no
        narrower. Wider refuses a credential that works; narrower passes one that cannot
        write, which is the whole failure being fixed. ``upload_dataframe`` is
        delete-then-insert, so this asks both questions, each with the real statement
        made harmless:

        * ``INSERT INTO <table> (<columns>) SELECT <columns> FROM <table> WHERE 1 = 0``
        * ``DELETE FROM <table> WHERE 1 = 0``, and only when :meth:`can_delete` is true,
          which is the same gate ``upload_dataframe`` puts the DELETE behind. On a table
          with no record-id column the upload skips the delete and warns, so asking for
          DELETE there would refuse a credential that never needs it.

        The engine runs the privilege check when it plans the statement, before any row
        is produced, so a refusal arrives while the row count is still zero. Nothing is
        written or removed even where the driver is in autocommit and the rollback below
        is a no-op, because neither statement has rows to commit. ``WHERE 1 = 0`` rather
        than ``WHERE FALSE`` because Teradata has no boolean literal; every dialect in
        this package accepts ``1 = 0``. Measured on postgres 16: the zero-row DELETE
        returns ``rowcount=0`` for a credential that holds the right and ``42501`` for
        one that does not.

        The DELETE names no column, which is deliberate and is what keeps the surface
        equal rather than wider. The real DELETE filters on the record-id column, so it
        needs SELECT on that column too -- and the INSERT probe above already asks for
        SELECT on every column, because ``get_table_columns()`` does.

        ``<columns>`` is :meth:`get_table_columns`, which is also what the uploader's own
        INSERT names: :meth:`_fit_to_schema` conforms the frame to exactly the table's
        columns before ``upload_dataframe`` reads ``df.columns`` off it. Deriving both
        from that one call is the point -- grants are per-column on every engine here, so
        a probe that asks for rights on more columns than the write path uses refuses
        credentials that would have worked. Naming the columns also keeps the two
        statements the same shape: a column-less ``INSERT INTO t SELECT * FROM t`` asks
        for INSERT on every column of the table, which stops matching the moment anyone
        narrows what the uploader writes.
        ``test_write_probe_columns_match_the_uploaders_insert`` pins that agreement.

        Both probes run even when the first is refused, so a credential short of both
        rights is told both at once instead of learning about the second only after
        fixing the first.

        The result is one-sided. A denial is raised only when
        :meth:`classify_write_denial` recognizes the driver's answer as an unambiguous
        refusal. Everything else passes: a timeout, a dropped connection, a table that
        does not exist, a schema or type error, an unrecognized driver error, a dialect
        with no classifier at all, an exception from the classifier itself. None of
        those establish that a working credential cannot write, and refusing on them
        would break destinations that work today. This method raises ``UserError`` and
        nothing else.

        It cannot see a rule evaluated per row -- a PostgreSQL row-level-security
        ``WITH CHECK`` policy, a Snowflake row access policy -- because a zero-row
        statement never reaches one. Those pass here and can still refuse the upload,
        which is the direction this is allowed to be wrong in.
        """
        try:
            columns = self.get_table_columns()
        except Exception as e:
            # Includes the table not existing. Not our question, and the uploader
            # surfaces it with its own message when the job runs.
            logger.info(
                f"write-permission check skipped, table schema unavailable: {safe_error_summary(e)}"
            )
            return
        if not columns:
            return

        probes = [("INSERT", self._insert_probe_statement(columns=columns))]
        try:
            deletes = self.can_delete()
        except Exception as e:
            logger.info(f"write-permission check inconclusive: {safe_error_summary(e)}")
            deletes = False
        if deletes:
            probes.append(("DELETE", self._delete_probe_statement()))

        reasons: list[str] = []
        for privilege, statement in probes:
            reason = self._run_write_probe(privilege=privilege, statement=statement)
            if reason is not None and reason not in reasons:
                reasons.append(reason)
        if reasons:
            raise UserError(" ".join(reasons))

    def _run_write_probe(self, privilege: str, statement: str) -> Optional[str]:
        """Run one zero-row probe. Return a confirmed refusal, else None. Never raises."""
        try:
            with self.get_cursor() as cursor:
                try:
                    logger.debug(f"running {privilege} permission probe: {statement}")
                    cursor.execute(statement)
                finally:
                    connection = getattr(cursor, "connection", None)
                    if connection is not None:
                        # Autocommit drivers, and drivers whose rollback fails on an
                        # already-aborted transaction. Zero rows either way.
                        with suppress(Exception):
                            connection.rollback()
        except Exception as e:
            try:
                reason = self.classify_write_denial(e, privilege=privilege)
            except Exception:
                reason = None
            if reason is None:
                logger.info(f"{privilege} permission check inconclusive: {safe_error_summary(e)}")
                return None
            logger.error(
                f"destination credentials cannot {privilege.lower()}: {safe_error_summary(e)}"
            )
            return reason
        return None

    def _quote_identifier(self, identifier: str) -> str:
        """Render a table or column name for the probe.

        Unquoted by default because that is how ``upload_dataframe`` renders the same
        names in the statements this probe stands in for. Overridden where the uploader
        quotes (teradata).
        """
        return identifier

    def _insert_probe_statement(self, columns: list[str]) -> str:
        table = self._quote_identifier(self.upload_config.table_name)
        column_list = ",".join(self._quote_identifier(column) for column in columns)
        return f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM {table} WHERE 1 = 0"

    def _delete_probe_statement(self) -> str:
        return f"DELETE FROM {self._quote_identifier(self.upload_config.table_name)} WHERE 1 = 0"

    def classify_write_denial(self, error: Exception, privilege: str) -> Optional[str]:
        """Return a user-facing reason iff this error is an unambiguous refusal.

        ``privilege`` is the right the refused statement needed, ``INSERT`` or
        ``DELETE``. It belongs in the message: telling a customer whose credential holds
        INSERT and not DELETE that they lack INSERT sends them to grant the wrong thing.

        Return ``None`` for everything else, including an error that may or may not be a
        privilege problem: PostgreSQL and Teradata both report "no such object" and "you
        may not see this object" with the same code, and guessing turns a wrong table
        name into a permissions accusation.

        A refusal here is NOT an authentication failure. The credential is valid and the
        connection is open; what is missing is a grant. Reporting it as an auth problem
        sends the customer off to rotate a working key, so the message must say which
        grant on which table, and the raised type is ``UserError`` (422) and never
        ``UserAuthError`` (401).

        Only the reason string is surfaced. Driver text is not: these messages routinely
        embed the host, user, and password from the connection string.

        The base returns ``None``, so a dialect without a verified denial code never
        refuses anything.
        """
        return None

    def _write_denied_message(self, privilege: str) -> str:
        return (
            f"The destination credentials can connect to the database but do not have "
            f"{privilege} permission on table '{self.upload_config.table_name}'. Records "
            f"would fail to write. Grant {privilege} on that table to the user this "
            f"connector authenticates as."
        )

    @contextmanager
    def get_cursor(self) -> Generator[Any, None, None]:
        with self.connection_config.get_cursor() as cursor:
            yield cursor

    def prepare_data(
        self, columns: list[str], data: tuple[tuple[Any, ...], ...]
    ) -> list[tuple[Any, ...]]:
        import pandas as pd

        output = []
        for row in data:
            parsed = []
            for column_name, value in zip(columns, row):
                if column_name in _DATE_COLUMNS:
                    if value is None or pd.isna(value):  # pandas is nan
                        parsed.append(None)
                    else:
                        parsed.append(parse_date_string(value))
                else:
                    parsed.append(value)
            output.append(tuple(parsed))
        return output

    def _fit_to_schema(
        self, df: "DataFrame", add_missing_columns: bool = True, case_sensitive: bool = True
    ) -> "DataFrame":
        import pandas as pd

        table_columns = self.get_table_columns()
        columns = set(df.columns if case_sensitive else df.columns.str.lower())
        schema_fields = set(
            table_columns if case_sensitive else {col.lower() for col in table_columns}
        )
        columns_to_drop = columns - schema_fields
        missing_columns = schema_fields - columns

        if columns_to_drop:
            logger.info(
                "Following columns will be dropped to match the table's schema: "
                f"{', '.join(columns_to_drop)}"
            )
        if missing_columns and add_missing_columns:
            logger.info(
                "Following null filled columns will be added to match the table's schema:"
                f" {', '.join(missing_columns)} "
            )

        df = df.drop(columns=columns_to_drop)

        if add_missing_columns:
            for column in missing_columns:
                df[column] = pd.Series()
        return df

    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        import numpy as np

        # A destination reachable at precheck() can still become unreachable
        # mid-upload (e.g. a network tunnel or the DB endpoint going down). Every
        # DB interaction below can trip on that: the schema probe (via
        # can_delete()/_fit_to_schema() -> get_table_columns()), the DELETE, and
        # the INSERT batches. Wrap the whole workflow so any such failure surfaces
        # as the same typed connection error precheck() raises (4xx/user) instead
        # of a bare driver exception that reads as 500/platform.
        try:
            if self.can_delete():
                self.delete_by_record_id(file_data=file_data)
            else:
                logger.warning(
                    f"table doesn't contain expected "
                    f"record id column "
                    f"{self.upload_config.record_id_key}, skipping delete"
                )
            df = self._fit_to_schema(df=df)
            df.replace({np.nan: None}, inplace=True)

            columns = list(df.columns)
            stmt = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
                table_name=self.upload_config.table_name,
                columns=",".join(columns),
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
                    values = self.prepare_data(
                        columns, tuple(rows.itertuples(index=False, name=None))
                    )
                    logger.debug(f"running query: {stmt}")
                    cursor.executemany(stmt, values)
        except (ImportError, UnstructuredIngestError):
            # Preserve dependency-install guidance and connector-authored typed
            # errors; only unexpected exceptions are redacted below.
            raise
        except Exception as e:
            logger.error(f"failed to upload: {safe_error_summary(e)}")
            raise DestinationConnectionError(
                f"failed to upload: {safe_error_summary(e)}"
            ) from None

    def get_table_columns(self) -> list[str]:
        if self._columns is None:
            with self.get_cursor() as cursor:
                cursor.execute(f"SELECT * from {self.upload_config.table_name} LIMIT 1")
                self._columns = [desc[0] for desc in cursor.description]
        return self._columns

    def can_delete(self) -> bool:
        return any(
            col.lower() == self.upload_config.record_id_key.lower()
            for col in self.get_table_columns()
        )

    def delete_by_record_id(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with data "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from table {self.upload_config.table_name}"
        )
        stmt = f"DELETE FROM {self.upload_config.table_name} WHERE {self.upload_config.record_id_key} = {self.values_delimiter}"  # noqa: E501
        with self.get_cursor() as cursor:
            cursor.execute(stmt, [file_data.identifier])
            rowcount = cursor.rowcount
            if rowcount > 0:
                logger.info(f"deleted {rowcount} rows from table {self.upload_config.table_name}")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        import pandas as pd

        df = pd.DataFrame(data)
        self.upload_dataframe(df=df, file_data=file_data)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        df = get_data_df(path=path)
        self.upload_dataframe(df=df, file_data=file_data)
