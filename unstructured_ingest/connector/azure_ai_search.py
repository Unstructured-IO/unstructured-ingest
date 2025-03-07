import json
import typing as t
import uuid
from dataclasses import dataclass, field

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.interfaces import (
    AccessConfig,
    BaseConnectorConfig,
    BaseDestinationConnector,
    WriteConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if t.TYPE_CHECKING:
    from azure.search.documents import SearchClient


@dataclass
class AzureAiSearchAccessConfig(AccessConfig):
    key: str = enhanced_field(sensitive=True)


@dataclass
class SimpleAzureAISearchStorageConfig(BaseConnectorConfig):
    endpoint: str
    access_config: AzureAiSearchAccessConfig


@dataclass
class AzureAISearchWriteConfig(WriteConfig):
    index: str


@dataclass
class AzureAISearchDestinationConnector(BaseDestinationConnector):
    write_config: AzureAISearchWriteConfig
    connector_config: SimpleAzureAISearchStorageConfig
    _client: t.Optional["SearchClient"] = field(init=False, default=None)

    @requires_dependencies(["azure.search"], extras="azure-ai-search")
    def generate_client(self) -> "SearchClient":
        from azure.core.credentials import AzureKeyCredential
        from azure.search.documents import SearchClient

        # Create a client
        credential = AzureKeyCredential(self.connector_config.access_config.key)
        return SearchClient(
            endpoint=self.connector_config.endpoint,
            index_name=self.write_config.index,
            credential=credential,
        )

    @property
    def client(self) -> "SearchClient":
        if self._client is None:
            self._client = self.generate_client()
        return self._client

    def check_connection(self):
        try:
            self.client.get_document_count()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def initialize(self):
        _ = self.client

    def conform_dict(self, data: dict) -> None:
        """
        updates the dictionary that is from each Element being converted into a dict/json
        into a dictionary that conforms to the schema expected by the
        Azure Cognitive Search index
        """
        from dateutil import parser  # type: ignore

        data["id"] = str(uuid.uuid4())

        if points := data.get("metadata", {}).get("coordinates", {}).get("points"):
            data["metadata"]["coordinates"]["points"] = json.dumps(points)
        if version := data.get("metadata", {}).get("data_source", {}).get("version"):
            data["metadata"]["data_source"]["version"] = str(version)
        if record_locator := data.get("metadata", {}).get("data_source", {}).get("record_locator"):
            data["metadata"]["data_source"]["record_locator"] = json.dumps(record_locator)
        if permissions_data := (
            data.get("metadata", {}).get("data_source", {}).get("permissions_data")
        ):
            data["metadata"]["data_source"]["permissions_data"] = json.dumps(permissions_data)
        if links := data.get("metadata", {}).get("links"):
            data["metadata"]["links"] = [json.dumps(link) for link in links]
        if last_modified := data.get("metadata", {}).get("last_modified"):
            data["metadata"]["last_modified"] = parser.parse(last_modified).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )
        if date_created := data.get("metadata", {}).get("data_source", {}).get("date_created"):
            data["metadata"]["data_source"]["date_created"] = parser.parse(date_created).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )
        if date_modified := data.get("metadata", {}).get("data_source", {}).get("date_modified"):
            data["metadata"]["data_source"]["date_modified"] = parser.parse(date_modified).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )
        if date_processed := data.get("metadata", {}).get("data_source", {}).get("date_processed"):
            data["metadata"]["data_source"]["date_processed"] = parser.parse(
                date_processed,
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if regex_metadata := data.get("metadata", {}).get("regex_metadata"):
            data["metadata"]["regex_metadata"] = json.dumps(regex_metadata)
        if page_number := data.get("metadata", {}).get("page_number"):
            data["metadata"]["page_number"] = str(page_number)

    @requires_dependencies(["azure"], extras="azure-ai-search")
    def write_dict(self, *args, elements_dict: t.List[t.Dict[str, t.Any]], **kwargs) -> None:
        import azure.core.exceptions

        logger.info(
            f"writing {len(elements_dict)} documents to destination "
            f"index at {self.write_config.index}",
        )
        try:
            results = self.client.upload_documents(documents=elements_dict)

        except azure.core.exceptions.HttpResponseError as http_error:
            raise WriteError(f"http error: {http_error}") from http_error
        errors = []
        success = []
        for result in results:
            if result.succeeded:
                success.append(result)
            else:
                errors.append(result)
        logger.debug(f"results: {len(success)} successes, {len(errors)} failures")
        if errors:
            raise WriteError(
                ", ".join(
                    [
                        f"{error.key}: [{error.status_code}] {error.error_message}"
                        for error in errors
                    ],
                ),
            )
