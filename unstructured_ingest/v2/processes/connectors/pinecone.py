import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.utils.data_prep import flatten_dict, generator_batching_wbytes
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
from unstructured_ingest.v2.processes.connector_registry import DestinationRegistryEntry

if TYPE_CHECKING:
    from pinecone import Index as PineconeIndex
    from pinecone import Pinecone


CONNECTOR_TYPE = "pinecone"
MAX_PAYLOAD_SIZE = 2 * 1024 * 1024  # 2MB
MAX_POOL_THREADS = 100
MAX_METADATA_BYTES = 40960  # 40KB https://docs.pinecone.io/reference/quotas-and-limits#hard-limits
MAX_QUERY_RESULTS = 10000


class PineconeAccessConfig(AccessConfig):
    pinecone_api_key: Optional[str] = Field(
        default=None, description="API key for Pinecone.", alias="api_key"
    )


class PineconeConnectionConfig(ConnectionConfig):
    index_name: str = Field(description="Name of the index to connect to.")
    access_config: Secret[PineconeAccessConfig] = Field(
        default=PineconeAccessConfig(), validate_default=True
    )

    @requires_dependencies(["pinecone"], extras="pinecone")
    def get_client(self, **index_kwargs) -> "Pinecone":
        from pinecone import Pinecone

        from unstructured_ingest import __version__ as unstructured_version

        return Pinecone(
            api_key=self.access_config.get_secret_value().pinecone_api_key,
            source_tag=f"unstructured_ingest=={unstructured_version}",
        )

    def get_index(self, **index_kwargs) -> "PineconeIndex":
        pc = self.get_client()

        index = pc.Index(name=self.index_name, **index_kwargs)
        logger.debug(f"connected to index: {pc.describe_index(self.index_name)}")
        return index


ALLOWED_FIELDS = (
    "element_id",
    "text",
    "parent_id",
    "category_depth",
    "emphasized_text_tags",
    "emphasized_text_contents",
    "coordinates",
    "last_modified",
    "page_number",
    "filename",
    "is_continuation",
    "link_urls",
    "link_texts",
    "text_as_html",
)


class PineconeUploadStagerConfig(UploadStagerConfig):
    metadata_fields: list[str] = Field(
        default=list(ALLOWED_FIELDS),
        description=(
            "which metadata from the source element to map to the payload metadata being sent to "
            "Pinecone."
        ),
    )


class PineconeUploaderConfig(UploaderConfig):
    batch_size: Optional[int] = Field(
        default=None,
        description="Optional number of records per batch. Will otherwise limit by size.",
    )
    pool_threads: Optional[int] = Field(
        default=1, description="Optional limit on number of threads to use for upload"
    )
    namespace: Optional[str] = Field(
        default=None,
        description="The namespace to write to. If not specified, the default namespace is used",
    )
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class PineconeUploadStager(UploadStager):
    upload_stager_config: PineconeUploadStagerConfig = field(
        default_factory=lambda: PineconeUploadStagerConfig()
    )

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        embeddings = element_dict.pop("embeddings", None)
        metadata: dict[str, Any] = element_dict.pop("metadata", {})
        data_source = metadata.pop("data_source", {})
        coordinates = metadata.pop("coordinates", {})
        pinecone_metadata = {}
        for possible_meta in [element_dict, metadata, data_source, coordinates]:
            pinecone_metadata.update(
                {
                    k: v
                    for k, v in possible_meta.items()
                    if k in self.upload_stager_config.metadata_fields
                }
            )

        metadata = flatten_dict(
            pinecone_metadata,
            separator="-",
            flatten_lists=True,
            remove_none=True,
        )
        metadata_size_bytes = len(json.dumps(metadata).encode())
        if metadata_size_bytes > MAX_METADATA_BYTES:
            logger.info(
                f"Metadata size is {metadata_size_bytes} bytes, which exceeds the limit of"
                f" {MAX_METADATA_BYTES} bytes per vector. Dropping the metadata."
            )
            metadata = {}

        metadata[RECORD_ID_LABEL] = file_data.identifier

        return {
            "id": str(uuid.uuid4()),
            "values": embeddings,
            "metadata": metadata,
        }

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
            self.conform_dict(element_dict=element, file_data=file_data)
            for element in elements_contents
        ]

        if Path(output_filename).suffix != ".json":
            output_filename = f"{output_filename}.json"
        else:
            output_filename = f"{Path(output_filename).stem}.json"
        output_path = Path(output_dir) / Path(f"{output_filename}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


@dataclass
class PineconeUploader(Uploader):
    upload_config: PineconeUploaderConfig
    connection_config: PineconeConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self):
        try:
            self.connection_config.get_index()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def pod_delete_by_record_id(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from pinecone pod index"
        )
        index = self.connection_config.get_index(pool_threads=MAX_POOL_THREADS)
        delete_kwargs = {
            "filter": {self.upload_config.record_id_key: {"$eq": file_data.identifier}}
        }
        if namespace := self.upload_config.namespace:
            delete_kwargs["namespace"] = namespace

        resp = index.delete(**delete_kwargs)
        logger.debug(
            f"deleted any content with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from pinecone index: {resp}"
        )

    def delete_by_query(self, index: "PineconeIndex", query_params: dict) -> None:
        while True:
            query_results = index.query(**query_params)
            matches = query_results.get("matches", [])
            if not matches:
                break
            ids = [match["id"] for match in matches]
            delete_params = {"ids": ids}
            if namespace := self.upload_config.namespace:
                delete_params["namespace"] = namespace
            index.delete(**delete_params)

    def serverless_delete_by_record_id(self, file_data: FileData) -> None:
        logger.debug(
            f"deleting any content with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from pinecone serverless index"
        )
        index = self.connection_config.get_index(pool_threads=MAX_POOL_THREADS)
        index_stats = index.describe_index_stats()
        dimension = index_stats["dimension"]
        total_vectors = index_stats["total_vector_count"]
        if total_vectors == 0:
            return
        while total_vectors > 0:
            top_k = min(total_vectors, MAX_QUERY_RESULTS)
            query_params = {
                "filter": {self.upload_config.record_id_key: {"$eq": file_data.identifier}},
                "vector": [0] * dimension,
                "top_k": top_k,
            }
            if namespace := self.upload_config.namespace:
                query_params["namespace"] = namespace
            self.delete_by_query(index=index, query_params=query_params)
            index_stats = index.describe_index_stats()
            total_vectors = index_stats["total_vector_count"]

        logger.info(
            f"deleted {total_vectors} records with metadata "
            f"{self.upload_config.record_id_key}={file_data.identifier} "
            f"from pinecone index"
        )

    @requires_dependencies(["pinecone"], extras="pinecone")
    def upsert_batches_async(self, elements_dict: list[dict]):
        from pinecone.exceptions import PineconeApiException

        chunks = list(
            generator_batching_wbytes(
                iterable=elements_dict,
                batch_size_limit_bytes=MAX_PAYLOAD_SIZE - 100,
                max_batch_size=self.upload_config.batch_size,
            )
        )
        logger.info(f"split doc with {len(elements_dict)} elements into {len(chunks)} batches")

        max_pool_threads = min(len(chunks), MAX_POOL_THREADS)
        if self.upload_config.pool_threads:
            pool_threads = min(self.upload_config.pool_threads, max_pool_threads)
        else:
            pool_threads = max_pool_threads
        index = self.connection_config.get_index(pool_threads=pool_threads)
        with index:
            upsert_kwargs = [{"vectors": chunk, "async_req": True} for chunk in chunks]
            if namespace := self.upload_config.namespace:
                for kwargs in upsert_kwargs:
                    kwargs["namespace"] = namespace
            async_results = [index.upsert(**kwarg) for kwarg in upsert_kwargs]
            # Wait for and retrieve responses (this raises in case of error)
            try:
                results = [async_result.get() for async_result in async_results]
            except PineconeApiException as api_error:
                raise DestinationConnectionError(f"http error: {api_error}") from api_error
            logger.debug(f"results: {results}")

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"writing a total of {len(elements_dict)} elements via"
            f" document batches to destination"
            f" index named {self.connection_config.index_name}"
        )
        # Determine if serverless or pod based index
        pinecone_client = self.connection_config.get_client()
        index_description = pinecone_client.describe_index(name=self.connection_config.index_name)
        if "serverless" in index_description.get("spec"):
            self.serverless_delete_by_record_id(file_data=file_data)
        elif "pod" in index_description.get("spec"):
            self.pod_delete_by_record_id(file_data=file_data)
        else:
            raise ValueError(f"unexpected spec type in index description: {index_description}")
        self.upsert_batches_async(elements_dict=elements_dict)


pinecone_destination_entry = DestinationRegistryEntry(
    connection_config=PineconeConnectionConfig,
    uploader=PineconeUploader,
    uploader_config=PineconeUploaderConfig,
    upload_stager=PineconeUploadStager,
    upload_stager_config=PineconeUploadStagerConfig,
)
