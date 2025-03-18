import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Generator, Tuple, Union

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import get_data_df
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.errors import ProviderError, UserAuthError, UserError
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.sql.sql import (
    SQLUploadStager,
    SQLUploadStagerConfig,
)

if TYPE_CHECKING:
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.table import Table, Transaction

CONNECTOR_TYPE = "ibm_watsonx_data"

DEFAULT_ICEBERG_URI_PATH = "/mds/iceberg"
DEFAULT_ICEBERG_CATALOG_TYPE = "rest"


@requires_dependencies(["pyiceberg"], extras="ibm-watsonx-data")
def _transaction_wrapper(table: "Table", fn: Callable, max_retries: int, **kwargs: Any) -> None:
    """
    Executes a function within a table transaction, with automatic retries on failure.

    Args:
        table (Table): The table object on which the transaction is performed.
        fn (Callable): The function to execute within the transaction.
        max_retries (int): The maximum number of retries allowed for the transaction.
        **kwargs (Any): Additional keyword arguments to pass to the function `fn`.

    Raises:
        DestinationConnectionError: If the transaction fails after the maximum number of retries.
    """
    from pyiceberg.exceptions import CommitFailedException

    current_retry = 0
    # Automatic retries are not available in pyiceberg
    # So, we are manually retrying failed transactions
    while current_retry < max_retries:
        try:
            with table.transaction() as transaction:
                fn(transaction, **kwargs)
            break
        except CommitFailedException:
            current_retry += 1
            table.refresh()
            logger.debug(
                f"Failed to commit transaction. Retrying! ({current_retry} / {max_retries})"
            )
        except Exception as e:
            raise DestinationConnectionError(f"Failed to append data to table: {e}")
    if current_retry >= max_retries:
        raise DestinationConnectionError(
            f"Failed to commit transaction after {max_retries} retries"
        )


class IbmWatsonxDataAccessConfig(AccessConfig):
    iam_api_key: str = Field(description="IBM IAM API Key")
    access_key_id: str = Field(description="Cloud Object Storage HMAC Access Key ID")
    secret_access_key: str = Field(description="Cloud Object Storage HMAC Secret Access Key")


class IbmWatsonxDataConnectionConfig(ConnectionConfig):
    access_config: Secret[IbmWatsonxDataAccessConfig]
    iceberg_endpoint: str = Field(description="Iceberg REST endpoint")
    object_storage_endpoint: str = Field(description="Cloud Object Storage public endpoint")
    object_storage_region: str = Field(description="Cloud Object Storage region")
    catalog: str = Field(description="Catalog name")

    _bearer_token: Union[dict[str, Any], None] = None

    @property
    def iceberg_url(self) -> str:
        return f"https://{self.iceberg_endpoint.strip("/")}{DEFAULT_ICEBERG_URI_PATH}"

    @property
    def object_storage_url(self) -> str:
        return f"https://{self.object_storage_endpoint.strip("/")}"

    @property
    def bearer_token(self) -> str:
        # Add 60 seconds to deal with edge cases where the token expires before the request is made
        timestamp = int(time.time()) + 60
        if self._bearer_token is None or self._bearer_token.get("expiration", 0) <= timestamp:
            self._bearer_token = self.generate_bearer_token()
        return self._bearer_token["access_token"]

    @requires_dependencies(["httpx"], extras="ibm-watsonx-data")
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
                f"Request to {url} failed"
                f"in IBM watsonx.data connector, status code {response_code}"
            )
            return UserError(e)
        if response_code > 500:
            logger.error(
                f"Request to {url} failed"
                f"in IBM watsonx.data connector, status code {response_code}"
            )
            return ProviderError(e)
        logger.error(f"Unhandled exception from IBM watsonx.data connector: {e}", exc_info=True)
        return e

    @requires_dependencies(["httpx"], extras="ibm-watsonx-data")
    def generate_bearer_token(self) -> dict[str, Any]:
        import httpx

        iam_url = "https://iam.cloud.ibm.com/identity/token"
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
            response = httpx.post(iam_url, headers=headers, data=data)
            response.raise_for_status()
        except Exception as e:
            raise self.wrap_error(e)
        return response.json()

    @requires_dependencies(["pyiceberg"], extras="ibm-watsonx-data")
    @contextmanager
    def get_catalog(self) -> Generator["RestCatalog", None, None]:
        from pyiceberg.catalog import load_catalog

        bearer_token = self.bearer_token
        try:
            catalog = load_catalog(
                self.catalog,
                **{
                    "type": DEFAULT_ICEBERG_CATALOG_TYPE,
                    "uri": self.iceberg_url,
                    "token": bearer_token,
                    "warehouse": self.catalog,
                    "s3.endpoint": self.object_storage_url,
                    "s3.access-key-id": self.access_config.get_secret_value().access_key_id,
                    "s3.secret-access-key": self.access_config.get_secret_value().secret_access_key,
                    "s3.region": self.object_storage_region,
                },
            )
        except Exception as e:
            logger.error(f"Failed to connect to catalog '{self.catalog}': {e}", exc_info=True)
            raise DestinationConnectionError(f"Failed to connect to catalog '{self.catalog}': {e}")

        yield catalog


@dataclass
class IbmWatsonxDataUploadStagerConfig(SQLUploadStagerConfig):
    pass


@dataclass
class IbmWatsonxDataUploadStager(SQLUploadStager):
    upload_stager_config: IbmWatsonxDataUploadStagerConfig = field(
        default_factory=IbmWatsonxDataUploadStagerConfig
    )


class IbmWatsonxDataUploaderConfig(UploaderConfig):
    namespace: str = Field(description="Namespace name")
    table: str = Field(description="Table name")
    max_retries: int = Field(
        default=3, description="Maximum number of retries to upload data", ge=2, le=5
    )
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="Searchable key to find entries for the same record on previous runs",
    )

    @property
    def table_identifier(self) -> Tuple[str, str]:
        return (self.namespace, self.table)


@dataclass
class IbmWatsonxDataUploader(Uploader):
    connection_config: IbmWatsonxDataConnectionConfig
    upload_config: IbmWatsonxDataUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        with self.connection_config.get_catalog() as catalog:
            if not catalog.namespace_exists(self.upload_config.namespace):
                raise DestinationConnectionError(
                    f"Namespace '{self.upload_config.namespace}' does not exist"
                )
            if not catalog.table_exists(self.upload_config.table_identifier):
                raise DestinationConnectionError(
                    f"Table '{self.upload_config.table}' does not exist in namespace '{self.upload_config.namespace}'"  # noqa: E501
                )

    @requires_dependencies(["pyarrow"], extras="ibm-watsonx-data")
    def _get_data_table(self, path: Path) -> Any:
        import pyarrow as pa

        df = get_data_df(path)
        return pa.Table.from_pandas(df)

    @contextmanager
    def get_table(self) -> Generator["Table", None, None]:
        with self.connection_config.get_catalog() as catalog:
            table = catalog.load_table(self.upload_config.table_identifier)
            yield table

    @requires_dependencies(["pyiceberg"], extras="ibm-watsonx-data")
    def upload_data(self, data_table: Any, file_data: FileData) -> None:
        from pyiceberg.expressions import EqualTo

        def _upload_data(transaction: "Transaction", data_table: Any, file_data: FileData) -> None:
            with transaction.update_schema() as update:
                update.union_by_name(data_table.schema)
            if self.upload_config.record_id_key in transaction._table.schema().column_names:
                transaction.delete(
                    delete_filter=EqualTo(self.upload_config.record_id_key, file_data.identifier)
                )
            transaction.append(data_table)

        with self.get_table() as table:
            _transaction_wrapper(
                table=table,
                fn=_upload_data,
                max_retries=self.upload_config.max_retries,
                file_data=file_data,
                data_table=data_table,
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        data_table = self._get_data_table(path)
        self.upload_data(data_table, file_data)


ibm_watsonx_data_destination_entry = DestinationRegistryEntry(
    connection_config=IbmWatsonxDataConnectionConfig,
    uploader=IbmWatsonxDataUploader,
    uploader_config=IbmWatsonxDataUploaderConfig,
    upload_stager=IbmWatsonxDataUploadStager,
    upload_stager_config=IbmWatsonxDataUploadStagerConfig,
)
