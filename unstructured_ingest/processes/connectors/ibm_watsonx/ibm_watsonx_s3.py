import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional, Tuple

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.errors_v2 import ProviderError, UserAuthError, UserError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploaderConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.processes.connectors.sql.sql import (
    SQLUploader,
    SQLUploadStager,
    SQLUploadStagerConfig,
)
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import get_data_df
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from pandas import DataFrame
    from pyarrow import Table as ArrowTable
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.table import Table, Transaction

CONNECTOR_TYPE = "ibm_watsonx_s3"

DEFAULT_IBM_CLOUD_AUTH_URL = "https://iam.cloud.ibm.com/identity/token"
DEFAULT_ICEBERG_URI_PATH = "/mds/iceberg"
DEFAULT_ICEBERG_CATALOG_TYPE = "rest"


class IcebergCommitFailedException(Exception):
    """Failed to commit changes to the iceberg table."""


class IbmWatsonxAccessConfig(AccessConfig):
    iam_api_key: str = Field(description="IBM IAM API Key")
    access_key_id: str = Field(description="Cloud Object Storage HMAC Access Key ID")
    secret_access_key: str = Field(description="Cloud Object Storage HMAC Secret Access Key")


class IbmWatsonxConnectionConfig(ConnectionConfig):
    access_config: Secret[IbmWatsonxAccessConfig]
    iceberg_endpoint: str = Field(description="Iceberg REST endpoint")
    object_storage_endpoint: str = Field(description="Cloud Object Storage public endpoint")
    object_storage_region: str = Field(description="Cloud Object Storage region")
    catalog: str = Field(description="Catalog name")
    max_retries_connection: int = Field(
        default=10,
        description="Maximum number of retries in case of a connection error (RESTError)",
        ge=2,
        le=100,
    )

    _bearer_token: Optional[dict[str, Any]] = None

    @property
    def iceberg_url(self) -> str:
        return f"https://{self.iceberg_endpoint.strip('/')}{DEFAULT_ICEBERG_URI_PATH}"

    @property
    def object_storage_url(self) -> str:
        return f"https://{self.object_storage_endpoint.strip('/')}"

    @property
    def bearer_token(self) -> str:
        # Add 5 minutes to deal with edge cases where the token expires before the request is made
        timestamp = int(time.time()) + (60 * 5)
        if self._bearer_token is None or self._bearer_token.get("expiration", 0) <= timestamp:
            self._bearer_token = self.generate_bearer_token()
        return self._bearer_token["access_token"]

    @requires_dependencies(["httpx"], extras="ibm-watsonx-s3")
    def wrap_error(self, e: Exception) -> Exception:
        import httpx

        if not isinstance(e, httpx.HTTPStatusError):
            logger.error(f"Unhandled exception from IBM watsonx.data connector: {e}", exc_info=True)
            return e
        url = e.request.url
        response_code = e.response.status_code
        if response_code == 401:
            logger.error(
                f"Failed to authenticate IBM watsonx.data user {url}, status code {response_code}"
            )
            return UserAuthError(e)
        if response_code == 403:
            logger.error(
                f"Given IBM watsonx.data user is not authorized {url}, status code {response_code}"
            )
            return UserAuthError(e)
        if 400 <= response_code < 500:
            logger.error(
                f"Request to {url} failedin IBM watsonx.data connector, status code {response_code}"
            )
            return UserError(e)
        if response_code > 500:
            logger.error(
                f"Request to {url} failedin IBM watsonx.data connector, status code {response_code}"
            )
            return ProviderError(e)
        logger.error(f"Unhandled exception from IBM watsonx.data connector: {e}", exc_info=True)
        return e

    @requires_dependencies(["httpx"], extras="ibm-watsonx-s3")
    def generate_bearer_token(self) -> dict[str, Any]:
        import httpx

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": self.access_config.get_secret_value().iam_api_key,
        }

        logger.info("Generating IBM IAM Bearer Token")
        try:
            response = httpx.post(DEFAULT_IBM_CLOUD_AUTH_URL, headers=headers, data=data)
            response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e)
        return response.json()

    def get_catalog_config(self) -> dict[str, Any]:
        return {
            "name": self.catalog,
            "type": DEFAULT_ICEBERG_CATALOG_TYPE,
            "uri": self.iceberg_url,
            "token": self.bearer_token,
            "warehouse": self.catalog,
            "s3.endpoint": self.object_storage_url,
            "s3.access-key-id": self.access_config.get_secret_value().access_key_id,
            "s3.secret-access-key": self.access_config.get_secret_value().secret_access_key,
            "s3.region": self.object_storage_region,
        }

    @requires_dependencies(["pyiceberg"], extras="ibm-watsonx-s3")
    @contextmanager
    def get_catalog(self) -> Generator["RestCatalog", None, None]:
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import RESTError
        from tenacity import (
            before_log,
            retry,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        # Retry connection in case of a connection error
        @retry(
            stop=stop_after_attempt(self.max_retries_connection),
            wait=wait_exponential(exp_base=2, multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(RESTError),
            before=before_log(logger, logging.DEBUG),
            reraise=True,
        )
        def _get_catalog(catalog_config: dict[str, Any]) -> "RestCatalog":
            return load_catalog(**catalog_config)

        try:
            catalog_config = self.get_catalog_config()
            catalog = _get_catalog(catalog_config)
        except Exception as e:
            logger.error(f"Failed to connect to catalog '{self.catalog}': {e}", exc_info=True)
            raise ProviderError(f"Failed to connect to catalog '{self.catalog}': {e}")

        yield catalog


@dataclass
class IbmWatsonxUploadStagerConfig(SQLUploadStagerConfig):
    pass


@dataclass
class IbmWatsonxUploadStager(SQLUploadStager):
    upload_stager_config: IbmWatsonxUploadStagerConfig = field(
        default_factory=IbmWatsonxUploadStagerConfig
    )


class IbmWatsonxUploaderConfig(UploaderConfig):
    namespace: str = Field(description="Namespace name")
    table: str = Field(description="Table name")
    max_retries: int = Field(
        default=50,
        description="Maximum number of retries to upload data (CommitFailedException)",
        ge=2,
        le=500,
    )
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="Searchable key to find entries for the same record on previous runs",
    )

    @property
    def table_identifier(self) -> Tuple[str, str]:
        return (self.namespace, self.table)


@dataclass
class IbmWatsonxUploader(SQLUploader):
    connection_config: IbmWatsonxConnectionConfig
    upload_config: IbmWatsonxUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        with self.connection_config.get_catalog() as catalog:
            if not catalog.namespace_exists(self.upload_config.namespace):
                raise UserError(f"Namespace '{self.upload_config.namespace}' does not exist")
            if not catalog.table_exists(self.upload_config.table_identifier):
                raise UserError(
                    f"Table '{self.upload_config.table}' does not exist in namespace '{self.upload_config.namespace}'"  # noqa: E501
                )

    @contextmanager
    def get_table(self) -> Generator["Table", None, None]:
        with self.connection_config.get_catalog() as catalog:
            table = catalog.load_table(self.upload_config.table_identifier)
            yield table

    def get_table_columns(self) -> list[str]:
        if self._columns is None:
            with self.get_table() as table:
                self._columns = table.schema().column_names
        return self._columns

    def can_delete(self) -> bool:
        return self.upload_config.record_id_key in self.get_table_columns()

    @requires_dependencies(["pyarrow"], extras="ibm-watsonx-s3")
    def _df_to_arrow_table(self, df: "DataFrame") -> "ArrowTable":
        import pyarrow as pa

        # Iceberg will automatically fill missing columns with nulls
        # Iceberg will throw an error if the DataFrame column has only null values
        # because it can't infer the type of the column and match it with the table schema
        return pa.Table.from_pandas(self._fit_to_schema(df, add_missing_columns=False))

    @requires_dependencies(["pyiceberg"], extras="ibm-watsonx-s3")
    def _delete(self, transaction: "Transaction", identifier: str) -> None:
        from pyiceberg.expressions import EqualTo

        if self.can_delete():
            transaction.delete(delete_filter=EqualTo(self.upload_config.record_id_key, identifier))
        else:
            logger.warning(
                f"Table doesn't contain expected "
                f"record id column "
                f"{self.upload_config.record_id_key}, skipping delete"
            )

    @requires_dependencies(["pyiceberg", "tenacity"], extras="ibm-watsonx-s3")
    def upload_data_table(
        self, table: "Table", data_table: "ArrowTable", file_data: FileData
    ) -> None:
        from pyiceberg.exceptions import CommitFailedException, RESTError
        from tenacity import (
            before_log,
            retry,
            retry_if_exception_type,
            stop_after_attempt,
            wait_random,
        )

        @retry(
            stop=stop_after_attempt(self.upload_config.max_retries),
            wait=wait_random(),
            retry=retry_if_exception_type(IcebergCommitFailedException),
            before=before_log(logger, logging.DEBUG),
            reraise=True,
        )
        def _upload_data_table(table: "Table", data_table: "ArrowTable", file_data: FileData):
            try:
                with table.transaction() as transaction:
                    self._delete(transaction, file_data.identifier)
                    transaction.append(data_table)
            except CommitFailedException as e:
                table.refresh()
                logger.debug(e)
                raise IcebergCommitFailedException(e)
            except RESTError:
                raise
            except Exception as e:
                raise ProviderError(f"Failed to upload data to table: {e}")

        try:
            return _upload_data_table(table, data_table, file_data)
        except RESTError:
            raise
        except ProviderError:
            raise
        except Exception as e:
            raise ProviderError(f"Failed to upload data to table: {e}")

    @requires_dependencies(["pyiceberg", "tenacity"], extras="ibm-watsonx-s3")
    def upload_dataframe(self, df: "DataFrame", file_data: FileData) -> None:
        from pyiceberg.exceptions import RESTError
        from tenacity import (
            before_log,
            retry,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        data_table = self._df_to_arrow_table(df)

        # Retry connection in case of a connection error or token expiration
        @retry(
            stop=stop_after_attempt(self.connection_config.max_retries_connection),
            wait=wait_exponential(exp_base=2, multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(RESTError),
            before=before_log(logger, logging.DEBUG),
            reraise=True,
        )
        def _upload_dataframe(data_table: Any, file_data: FileData) -> None:
            with self.get_table() as table:
                self.upload_data_table(table, data_table, file_data)

        try:
            return _upload_dataframe(data_table, file_data)
        except ProviderError:
            raise
        except Exception as e:
            raise ProviderError(f"Failed to upload data to table: {e}")

    @requires_dependencies(["pandas"], extras="ibm-watsonx-s3")
    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        import pandas as pd

        df = pd.DataFrame(data)
        self.upload_dataframe(df=df, file_data=file_data)

    @requires_dependencies(["pandas"], extras="ibm-watsonx-s3")
    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        df = get_data_df(path=path)
        self.upload_dataframe(df=df, file_data=file_data)


ibm_watsonx_s3_destination_entry = DestinationRegistryEntry(
    connection_config=IbmWatsonxConnectionConfig,
    uploader=IbmWatsonxUploader,
    uploader_config=IbmWatsonxUploaderConfig,
    upload_stager=IbmWatsonxUploadStager,
    upload_stager_config=IbmWatsonxUploadStagerConfig,
)
