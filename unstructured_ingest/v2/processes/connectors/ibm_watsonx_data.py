from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Tuple

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import get_data_df
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
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
    from pyiceberg.catalog.rest import RestCatalog

CONNECTOR_TYPE = "ibm_watsonx_data"

DEFAULT_ICEBERG_URI_PATH = "/mds/iceberg"
DEFAULT_ICEBERG_CATALOG_TYPE = "rest"


class IbmWatsonxDataAccessConfig(AccessConfig):
    iam_api_key: str = Field(description="")
    access_key_id: str = Field(description="")
    secret_access_key: str = Field(description="")


class IbmWatsonxDataConnectionConfig(ConnectionConfig):
    access_config: Secret[IbmWatsonxDataAccessConfig]
    iceberg_endpoint: str = Field(description="")
    object_storage_public_endpoint: str = Field(description="")
    object_storage_region: str = Field(description="")
    catalog: str = Field(description="Catalog name")

    @property
    def iceberg_url(self) -> str:
        return f"https://{self.iceberg_endpoint.strip("/")}{DEFAULT_ICEBERG_URI_PATH}"

    @property
    def object_storage_url(self) -> str:
        return f"https://{self.object_storage_public_endpoint.strip("/")}"

    @requires_dependencies(["requests"], extras="ibm-watsonx-data")
    def generate_bearer_token(self) -> str:
        import requests

        iam_url = "https://iam.cloud.ibm.com/identity/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "urn:ibm:params:oauth:grant-type:apikey",
            "apikey": self.access_config.get_secret_value().iam_api_key,
        }

        response = requests.post(iam_url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()["access_token"]

    @requires_dependencies(["pyiceberg"], extras="ibm-watsonx-data")
    def get_catalog(self) -> "RestCatalog":
        from pyiceberg.catalog import load_catalog

        try:
            catalog = load_catalog(
                self.catalog,
                **{
                    "type": DEFAULT_ICEBERG_CATALOG_TYPE,
                    "uri": self.iceberg_url,
                    "token": self.generate_bearer_token(),
                    "warehouse": self.catalog,
                    "s3.endpoint": self.object_storage_url,
                    "s3.access-key-id": self.access_config.get_secret_value().access_key_id,
                    "s3.secret-access-key": self.access_config.get_secret_value().secret_access_key,
                    "s3.region": self.object_storage_region,
                },
            )
            return catalog
        except Exception as e:
            logger.error(f"Failed to connect to catalog '{self.catalog}': {e}", exc_info=True)
            raise DestinationConnectionError(f"Failed to connect to catalog '{self.catalog}': {e}")


@dataclass
class IbmWatsonxDataUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class IbmWatsonxDataUploadStager(UploadStager):
    pass


class IbmWatsonxDataUploaderConfig(UploaderConfig):
    namespace: str = Field(description="Namespace name")
    table: str = Field(description="Table name")

    @property
    def table_identifier(self) -> Tuple[str, str]:
        return (self.namespace, self.table)


@dataclass
class IbmWatsonxDataUploader(Uploader):
    connection_config: IbmWatsonxDataConnectionConfig
    upload_config: IbmWatsonxDataUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        catalog = self.connection_config.get_catalog()
        if not catalog.namespace_exists(self.upload_config.namespace):
            err_msg = f"Namespace {self.upload_config.namespace} does not exist"
            logger.error(err_msg)
            raise DestinationConnectionError(err_msg)
        if not catalog.table_exists(self.upload_config.table_identifier):
            err_msg = f"Table {self.upload_config.table} does not exist in namespace {self.upload_config.namespace}"
            logger.error(err_msg)
            raise DestinationConnectionError(err_msg)

    @requires_dependencies(["pyarrow"], extras="ibm-watsonx-data")
    def _get_data_table(self, path: Path) -> Any:
        import pyarrow as pa

        df = get_data_df(path)
        return pa.Table.from_pandas(df)

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        catalog = self.connection_config.get_catalog()

        data_table = self._get_data_table(path)

        try:
            table = catalog.load_table(self.upload_config.table_identifier)
            table.append(data_table)
        except Exception as e:
            logger.error(f"Failed to append data to table: {e}", exc_info=True)
            raise DestinationConnectionError(f"Failed to append data to table: {e}")


ibm_watsonx_data_destination_entry = DestinationRegistryEntry(
    connection_config=IbmWatsonxDataConnectionConfig,
    uploader=IbmWatsonxDataUploader,
    uploader_config=IbmWatsonxDataUploaderConfig,
    upload_stager=IbmWatsonxDataUploadStager,
    upload_stager_config=IbmWatsonxDataUploadStagerConfig,
)
