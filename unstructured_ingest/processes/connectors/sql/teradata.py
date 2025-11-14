"""Teradata SQL connector for Unstructured Ingest.

This connector provides source (read) and destination (write) capabilities for Teradata databases.
It follows the DBAPI 2.0 standard using the teradatasql driver.

Phase 1 Implementation: Minimal viable connector with only required parameters.
- Connection: host, user, password (uses defaults for port 1025, TD2 auth)
- Source: Index and download records in batches
- Destination: Upload processed data with upsert behavior

Important Teradata-Specific Notes:
-------------------------------------
1. RESERVED WORDS: Teradata has many reserved keywords that cannot be used as column names
   without quoting. Most notably:
   - "type" is a RESERVED WORD in Teradata
   - When creating tables, use quoted identifiers: CREATE TABLE t ("type" VARCHAR(50))
   - The base Unstructured schema uses "type" column for element types (Title, NarrativeText, etc.)
   - Your destination table MUST use quoted "type" to preserve this data
   
2. SQL SYNTAX DIFFERENCES:
   - Teradata uses TOP instead of LIMIT: SELECT TOP 10 * FROM table
   - Teradata uses DATABASE instead of CURRENT_DATABASE
   - Teradata uses USER instead of CURRENT_USER
   
3. PARAMETER STYLE:
   - Uses qmark paramstyle: ? placeholders for prepared statements
   - Example: INSERT INTO table VALUES (?, ?, ?)

Example table schema with quoted "type" column:
    CREATE TABLE elements (
        id VARCHAR(64) NOT NULL,
        record_id VARCHAR(64),
        text VARCHAR(32000),
        "type" VARCHAR(50),  -- MUST be quoted! Reserved word in Teradata
        PRIMARY KEY (id)
    )
"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generator

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
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
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from pandas import DataFrame
    from teradatasql import TeradataConnection
    from teradatasql import TeradataCursor

CONNECTOR_TYPE = "teradata"


class TeradataAccessConfig(SQLAccessConfig):
    """Access configuration for Teradata authentication.

    Stores sensitive credentials (password) separately from connection config.
    """

    password: str = Field(description="Teradata user password")


class TeradataConnectionConfig(SQLConnectionConfig):
    """Connection configuration for Teradata database.

    Minimal Phase 1 implementation with only required parameters:
    - host: Teradata server hostname or IP
    - user: Database username
    - password: Database password (in access_config)

    Uses defaults:
    - Port: 1025 (Teradata default)
    - Authentication: TD2 (Teradata default)
    - Database: None (uses user default)
    """

    access_config: Secret[TeradataAccessConfig]
    host: str = Field(description="Teradata server hostname or IP address")
    user: str = Field(description="Teradata database username")
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    @contextmanager
    @requires_dependencies(["teradatasql"], extras="teradata")
    def get_connection(self) -> Generator["TeradataConnection", None, None]:
        """Create a connection to Teradata database.

        Uses teradatasql driver with DBAPI 2.0 interface.
        Connection automatically commits on success and closes on exit.

        Yields:
            TeradataConnection: Active database connection
        """
        from teradatasql import connect

        connection = connect(
            host=self.host,
            user=self.user,
            password=self.access_config.get_secret_value().password,
        )
        try:
            yield connection
        finally:
            connection.commit()
            connection.close()

    @contextmanager
    def get_cursor(self) -> Generator["TeradataCursor", None, None]:
        """Create a cursor from the connection.

        Yields:
            TeradataCursor: Database cursor for executing queries
        """
        with self.get_connection() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


# Indexer - discovers records in source table
class TeradataIndexerConfig(SQLIndexerConfig):
    """Configuration for Teradata indexer (inherits all from base)."""

    pass


@dataclass
class TeradataIndexer(SQLIndexer):
    """Indexes records from Teradata table.

    Discovers all record IDs and groups them into batches for download.
    Inherits all functionality from SQLIndexer base class.
    """

    connection_config: TeradataConnectionConfig
    index_config: TeradataIndexerConfig
    connector_type: str = CONNECTOR_TYPE


# Downloader - downloads batches of records
class TeradataDownloaderConfig(SQLDownloaderConfig):
    """Configuration for Teradata downloader (inherits all from base)."""

    pass


@dataclass
class TeradataDownloader(SQLDownloader):
    """Downloads batches of records from Teradata.

    Executes SELECT queries to fetch record batches and saves as CSV files.
    Uses qmark paramstyle (?) for query parameters.
    """

    connection_config: TeradataConnectionConfig
    download_config: TeradataDownloaderConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"  # Teradata uses qmark paramstyle

    def query_db(self, file_data: SqlBatchFileData) -> tuple[list[tuple], list[str]]:
        """Execute SELECT query to fetch batch of records.

        Args:
            file_data: Batch metadata with table name, ID column, and record IDs

        Returns:
            Tuple of (rows, column_names) from query result
        """
        table_name = file_data.additional_metadata.table_name
        id_column = file_data.additional_metadata.id_column
        ids = [item.identifier for item in file_data.batch_items]

        with self.connection_config.get_cursor() as cursor:
            # Build field selection
            fields = ",".join(self.download_config.fields) if self.download_config.fields else "*"

            # Build parameterized query with ? placeholders
            placeholders = ",".join([self.values_delimiter for _ in ids])
            query = f"SELECT {fields} FROM {table_name} WHERE {id_column} IN ({placeholders})"

            # Execute query with parameter binding
            cursor.execute(query, ids)
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]
            return rows, columns


# Upload Stager - prepares data for upload
class TeradataUploadStagerConfig(SQLUploadStagerConfig):
    """Configuration for Teradata upload stager (inherits all from base)."""

    pass


class TeradataUploadStager(SQLUploadStager):
    """Stages data for upload to Teradata.

    Transforms processed JSON elements into database-ready format.
    Inherits all functionality from SQLUploadStager base class.

    Overrides conform_dataframe() to handle Teradata-specific type conversions
    for the teradatasql driver, which is stricter than other DBAPI drivers.
    """

    upload_stager_config: TeradataUploadStagerConfig

    def conform_dataframe(self, df: "DataFrame") -> "DataFrame":
        """Convert DataFrame columns to Teradata-compatible types.

        Extends base class method to handle additional columns that teradatasql
        driver cannot serialize. The teradatasql driver is stricter about types
        than other DBAPI drivers and will raise TypeError for Python lists/dicts.

        Specifically adds 'languages' to the list of columns that must be
        JSON-stringified before insertion.

        Args:
            df: DataFrame with unstructured elements

        Returns:
            DataFrame with all columns converted to database-compatible types

        Raises:
            TypeError: If teradatasql driver receives incompatible Python types
        """
        import json

        # Call parent class method first (handles dates, standard JSON columns, etc.)
        df = super().conform_dataframe(df)

        # TODO: Double check this with other sql connectors
        # Teradata-specific: Convert 'languages' array to JSON string
        # teradatasql driver cannot handle Python lists in executemany()
        if "languages" in df.columns:
            df["languages"] = df["languages"].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else None
            )

        return df


# Uploader - uploads data to destination table
class TeradataUploaderConfig(SQLUploaderConfig):
    """Configuration for Teradata uploader (inherits all from base)."""

    pass


@dataclass
class TeradataUploader(SQLUploader):
    """Uploads processed data to Teradata table.

    Executes INSERT statements to load data in batches.
    Implements upsert behavior (delete existing records, then insert new).
    Uses qmark paramstyle (?) for query parameters.
    """

    upload_config: TeradataUploaderConfig = field(default_factory=TeradataUploaderConfig)
    connection_config: TeradataConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    values_delimiter: str = "?"  # Teradata uses qmark paramstyle

    def get_table_columns(self) -> list[str]:
        """Get column names from Teradata table.

        Overrides base method to use Teradata-specific TOP syntax instead of LIMIT.

        Returns:
            List of column names from the table
        """
        if self._columns is None:
            with self.get_cursor() as cursor:
                # Teradata uses TOP instead of LIMIT
                cursor.execute(f"SELECT TOP 1 * FROM {self.upload_config.table_name}")
                self._columns = [desc[0] for desc in cursor.description]
        return self._columns

    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        """Upload DataFrame to Teradata with quoted column names.

        Overrides base method to quote all column names in INSERT statement
        because Teradata has many reserved keywords (type, date, user, etc.)
        that must be quoted when used as column names.

        Args:
            df: DataFrame containing the data to upload
            file_data: Metadata about the file being processed

        Raises:
            OperationalError: If INSERT fails due to schema mismatch or syntax error
        """
        import numpy as np

        from unstructured_ingest.logger import logger
        from unstructured_ingest.utils.data_prep import split_dataframe

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
        
        # CRITICAL: Quote all column names for Teradata!
        # Teradata has MANY reserved keywords (type, date, user, time, etc.)
        # that cause "Syntax error, expected something like '('" if unquoted
        quoted_columns = [f'"{col}"' for col in columns]
        
        stmt = "INSERT INTO {table_name} ({columns}) VALUES({values})".format(
            table_name=self.upload_config.table_name,
            columns=",".join(quoted_columns),  # Use quoted column names
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


# Registry entries for connector discovery
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

