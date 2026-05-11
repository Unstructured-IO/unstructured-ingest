import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generator, TypeAlias

from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import DestinationConnectionError, ValueError, WriteError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
)
from unstructured_ingest.processes.connectors.utils import parse_datetime
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import batch_generator, get_enhanced_element_id
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from azure.search.documents import SearchClient
    from azure.search.documents.indexes import SearchIndexClient
    from azure.search.documents.indexes.models import SearchField, SearchIndex

CONNECTOR_TYPE = "azure_ai_search"

# Azure caps complex-type nesting at 10 levels
_MAX_INDEX_FIELD_DEPTH = 10

# Recursive map of the index schema: leaf scalars / primitive collections are ``None``;
# complex / collection-of-complex sub-trees are nested ``FieldTree`` dicts.
FieldTree: TypeAlias = "dict[str, FieldTree | None]"


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
    @contextmanager
    def get_search_client(self) -> Generator["SearchClient", None, None]:
        from azure.core.credentials import AzureKeyCredential
        from azure.search.documents import SearchClient

        with SearchClient(
            endpoint=self.endpoint,
            index_name=self.index,
            credential=AzureKeyCredential(
                self.access_config.get_secret_value().azure_ai_search_key
            ),
        ) as client:
            yield client

    @requires_dependencies(["azure.search", "azure.core"], extras="azure-ai-search")
    @contextmanager
    def get_search_index_client(self) -> Generator["SearchIndexClient", None, None]:
        from azure.core.credentials import AzureKeyCredential
        from azure.search.documents.indexes import SearchIndexClient

        with SearchIndexClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(
                self.access_config.get_secret_value().azure_ai_search_key
            ),
        ) as search_index_client:
            yield search_index_client


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

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        """
        updates the dictionary that is from each Element being converted into a dict/json
        into a dictionary that conforms to the schema expected by the
        Azure Cognitive Search index
        """
        data = element_dict.copy()
        data["id"] = get_enhanced_element_id(element_dict=data, file_data=file_data)
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


@dataclass
class AzureAISearchUploader(Uploader):
    upload_config: AzureAISearchUploaderConfig
    connection_config: AzureAISearchConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def query_docs(self, record_id: str, index_key: str) -> list[str]:
        with self.connection_config.get_search_client() as search_client:
            results = list(
                search_client.search(filter=f"record_id eq '{record_id}'", select=[index_key])
            )
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
        with self.connection_config.get_search_client() as search_client:
            results = search_client.delete_documents(
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
    def write_dict(
        self, elements_dict: list[dict[str, Any]], search_client: "SearchClient"
    ) -> None:
        import azure.core.exceptions

        logger.info(
            f"writing {len(elements_dict)} documents to destination "
            f"index at {self.connection_config.index}",
        )
        try:
            results = search_client.upload_documents(documents=elements_dict)
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

    def get_index(self):
        with self.connection_config.get_search_index_client() as search_index_client:
            return search_index_client.get_index(name=self.connection_config.index)

    def can_delete(self, index) -> bool:
        record_id_fields = [
            field for field in index.fields if field.name == self.upload_config.record_id_key
        ]
        if not record_id_fields:
            return False
        return record_id_fields[0].filterable

    def get_index_key(self, index) -> str:
        key_fields = [field for field in index.fields if field.key]
        if not key_fields:
            raise ValueError("no key field found in index fields")
        return key_fields[0].name

    def get_index_field_names(self, index) -> set[str]:
        return {field.name for field in index.fields}

    def filter_doc(self, doc: dict, index_field_names: set[str]) -> dict:
        return {k: v for k, v in doc.items() if k in index_field_names}

    def get_index_field_tree(self, index: "SearchIndex") -> FieldTree:
        """Build a nested tree mirroring the destination index schema."""
        return self._build_field_tree(index.fields, depth=0)

    def _build_field_tree(
        self, fields: "list[SearchField] | None", depth: int
    ) -> FieldTree:
        if depth >= _MAX_INDEX_FIELD_DEPTH:
            return {}
        tree: FieldTree = {}
        for f in fields or []:
            sub_fields = getattr(f, "fields", None)
            if sub_fields:
                tree[f.name] = self._build_field_tree(sub_fields, depth + 1)
            else:
                tree[f.name] = None
        return tree

    def filter_doc_against_tree(
        self, doc: dict[str, Any], tree: FieldTree
    ) -> dict[str, Any]:
        """Drop any field in ``doc`` not declared in ``tree``, recursing into complex types."""
        out: dict[str, Any] = {}
        for key, value in doc.items():
            if key not in tree:
                continue
            sub_tree = tree[key]
            if sub_tree is None:
                # Leaf: pass value through; a dict mapped to Edm.String stays as-is and
                # the SDK surfaces any genuine type mismatch.
                out[key] = value
            elif isinstance(value, dict):
                out[key] = self.filter_doc_against_tree(value, sub_tree)
            elif isinstance(value, list):
                out[key] = [
                    self.filter_doc_against_tree(item, sub_tree)
                    if isinstance(item, dict)
                    else item
                    for item in value
                ]
            else:
                out[key] = value
        return out

    def collect_dropped_paths(
        self, doc: dict[str, Any], tree: FieldTree, prefix: str = ""
    ) -> list[str]:
        """Return sorted dotted paths in ``doc`` not declared in ``tree``
        (e.g. ``metadata.table_extraction_method``)."""
        dropped: list[str] = []
        for key, value in doc.items():
            path = f"{prefix}.{key}" if prefix else key
            if key not in tree:
                dropped.append(path)
                continue
            sub_tree = tree[key]
            if sub_tree is None:
                continue
            if isinstance(value, dict):
                dropped.extend(self.collect_dropped_paths(value, sub_tree, path))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        dropped.extend(self.collect_dropped_paths(item, sub_tree, path))
        return sorted(set(dropped))

    def precheck(self) -> None:
        try:
            with self.connection_config.get_search_client() as search_client:
                search_client.get_document_count()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        logger.info(
            f"writing document batches to destination"
            f" endpoint at {str(self.connection_config.endpoint)}"
            f" index at {str(self.connection_config.index)}"
            f" with batch size {str(self.upload_config.batch_size)}"
        )
        index = self.get_index()
        if self.can_delete(index):
            index_key = self.get_index_key(index)
            self.delete_by_record_id(file_data=file_data, index_key=index_key)
        else:
            logger.warning("criteria for deleting previous content not met, skipping")

        field_tree = self.get_index_field_tree(index)
        if data:
            dropped_paths = self.collect_dropped_paths(data[0], field_tree)
            if dropped_paths:
                logger.info(
                    "Following fields will be dropped to match the index schema: "
                    f"{', '.join(dropped_paths)}"
                )
        filtered_data = [
            self.filter_doc_against_tree(doc=doc, tree=field_tree) for doc in data
        ]

        batch_size = self.upload_config.batch_size
        with self.connection_config.get_search_client() as search_client:
            for chunk in batch_generator(filtered_data, batch_size):
                self.write_dict(elements_dict=chunk, search_client=search_client)  # noqa: E203


azure_ai_search_destination_entry = DestinationRegistryEntry(
    connection_config=AzureAISearchConnectionConfig,
    uploader=AzureAISearchUploader,
    uploader_config=AzureAISearchUploaderConfig,
    upload_stager=AzureAISearchUploadStager,
    upload_stager_config=AzureAISearchUploadStagerConfig,
)
