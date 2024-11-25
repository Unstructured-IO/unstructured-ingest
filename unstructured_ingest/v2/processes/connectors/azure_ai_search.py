import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
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
from unstructured_ingest.v2.processes.connectors.utils import parse_datetime

if TYPE_CHECKING:
    from azure.search.documents import SearchClient
    from azure.search.documents.indexes import SearchIndexClient

CONNECTOR_TYPE = "azure_ai_search"


class AzureAISearchAccessConfig(AccessConfig):
    azure_ai_search_key: str = Field(
        alias="key", description="Credential that is used for authenticating to an Azure service"
    )


class AzureAISearchConnectionConfig(ConnectionConfig):
    endpoint: str = Field(
        description="The URL endpoint of an Azure AI (Cognitive) search service. "
        "In the form of https://{{service_name}}.search.windows.net"
    )
    index: str = Field(
        description="The name of the Azure AI (Cognitive) Search index to connect to."
    )
    access_config: Secret[AzureAISearchAccessConfig]

    @requires_dependencies(["azure.search", "azure.core"], extras="azure-ai-search")
    def get_search_client(self) -> "SearchClient":
        from azure.core.credentials import AzureKeyCredential
        from azure.search.documents import SearchClient

        return SearchClient(
            endpoint=self.endpoint,
            index_name=self.index,
            credential=AzureKeyCredential(
                self.access_config.get_secret_value().azure_ai_search_key
            ),
        )

    @requires_dependencies(["azure.search", "azure.core"], extras="azure-ai-search")
    def get_search_index_client(self) -> "SearchIndexClient":
        from azure.core.credentials import AzureKeyCredential
        from azure.search.documents.indexes import SearchIndexClient

        return SearchIndexClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(
                self.access_config.get_secret_value().azure_ai_search_key
            ),
        )


class AzureAISearchUploadStagerConfig(UploadStagerConfig):
    pass


class AzureAISearchUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=100, description="Number of records per batch")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class AzureAISearchUploadStager(UploadStager):
    upload_stager_config: AzureAISearchUploadStagerConfig = field(
        default_factory=lambda: AzureAISearchUploadStagerConfig()
    )

    @staticmethod
    def conform_dict(data: dict, file_data: FileData) -> dict:
        """
        updates the dictionary that is from each Element being converted into a dict/json
        into a dictionary that conforms to the schema expected by the
        Azure Cognitive Search index
        """

        data["id"] = str(uuid.uuid4())
        data[RECORD_ID_LABEL] = file_data.identifier

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
            data["metadata"]["last_modified"] = parse_datetime(last_modified).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        if date_created := data.get("metadata", {}).get("data_source", {}).get("date_created"):
            data["metadata"]["data_source"]["date_created"] = parse_datetime(date_created).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )

        if date_modified := data.get("metadata", {}).get("data_source", {}).get("date_modified"):
            data["metadata"]["data_source"]["date_modified"] = parse_datetime(
                date_modified
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if date_processed := data.get("metadata", {}).get("data_source", {}).get("date_processed"):
            data["metadata"]["data_source"]["date_processed"] = parse_datetime(
                date_processed
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        if regex_metadata := data.get("metadata", {}).get("regex_metadata"):
            data["metadata"]["regex_metadata"] = json.dumps(regex_metadata)
        if page_number := data.get("metadata", {}).get("page_number"):
            data["metadata"]["page_number"] = str(page_number)
        return data

    def run(
        self,
        file_data: FileData,
        elements_filepath: Path,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        conformed_elements = [
            self.conform_dict(data=element, file_data=file_data) for element in elements_contents
        ]

        if Path(output_filename).suffix != ".json":
            output_filename = f"{output_filename}.json"
        else:
            output_filename = f"{Path(output_filename).stem}.json"
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file, indent=2)
        return output_path


@dataclass
class AzureAISearchUploader(Uploader):
    upload_config: AzureAISearchUploaderConfig
    connection_config: AzureAISearchConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def query_docs(self, record_id: str, index_key: str) -> list[str]:
        client = self.connection_config.get_search_client()
        results = list(client.search(filter=f"record_id eq '{record_id}'", select=[index_key]))
        return [result[index_key] for result in results]

    def delete_by_record_id(self, file_data: FileData, index_key: str) -> None:
        logger.debug(
            f"deleting any content with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from azure cognitive search index: {self.connection_config.index}"
        )
        doc_ids_to_delete = self.query_docs(record_id=file_data.identifier, index_key=index_key)
        if not doc_ids_to_delete:
            return
        client: SearchClient = self.connection_config.get_search_client()
        results = client.delete_documents(
            documents=[{index_key: doc_id} for doc_id in doc_ids_to_delete]
        )
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
                    [f"[{error.status_code}] {error.error_message}" for error in errors],
                ),
            )

    @DestinationConnectionError.wrap
    @requires_dependencies(["azure"], extras="azure-ai-search")
    def write_dict(self, elements_dict: list[dict[str, Any]]) -> None:
        import azure.core.exceptions

        logger.info(
            f"writing {len(elements_dict)} documents to destination "
            f"index at {self.connection_config.index}",
        )
        try:
            results = self.connection_config.get_search_client().upload_documents(
                documents=elements_dict
            )

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
                        f"{error.azure_ai_search_key}: "
                        f"[{error.status_code}] {error.error_message}"
                        for error in errors
                    ],
                ),
            )

    def can_delete(self) -> bool:
        search_index_client = self.connection_config.get_search_index_client()
        index = search_index_client.get_index(name=self.connection_config.index)
        index_fields = index.fields
        record_id_fields = [
            field for field in index_fields if field.name == self.upload_config.record_id_key
        ]
        if not record_id_fields:
            return False
        record_id_field = record_id_fields[0]
        return record_id_field.filterable

    def get_index_key(self) -> str:
        search_index_client = self.connection_config.get_search_index_client()
        index = search_index_client.get_index(name=self.connection_config.index)
        index_fields = index.fields
        key_fields = [field for field in index_fields if field.key]
        if not key_fields:
            raise ValueError("no key field found in index fields")
        return key_fields[0].name

    def precheck(self) -> None:
        try:
            client = self.connection_config.get_search_client()
            client.get_document_count()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing document batches to destination"
            f" endpoint at {str(self.connection_config.endpoint)}"
            f" index at {str(self.connection_config.index)}"
            f" with batch size {str(self.upload_config.batch_size)}"
        )
        if self.can_delete():
            index_key = self.get_index_key()
            self.delete_by_record_id(file_data=file_data, index_key=index_key)
        else:
            logger.warning("criteria for deleting previous content not met, skipping")

        batch_size = self.upload_config.batch_size
        for chunk in batch_generator(elements_dict, batch_size):
            self.write_dict(elements_dict=chunk)  # noqa: E203


azure_ai_search_destination_entry = DestinationRegistryEntry(
    connection_config=AzureAISearchConnectionConfig,
    uploader=AzureAISearchUploader,
    uploader_config=AzureAISearchUploaderConfig,
    upload_stager=AzureAISearchUploadStager,
    upload_stager_config=AzureAISearchUploadStagerConfig,
)
