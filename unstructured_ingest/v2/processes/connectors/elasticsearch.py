import hashlib
import json
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional, Union

from pydantic import BaseModel, Field, Secret, SecretStr

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import flatten_dict, generator_batching_wbytes
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)

if TYPE_CHECKING:
    from elasticsearch import Elasticsearch as ElasticsearchClient

CONNECTOR_TYPE = "elasticsearch"


class ElasticsearchAccessConfig(AccessConfig):
    password: Optional[str] = Field(
        default=None, description="password when using basic auth or connecting to a cloud instance"
    )
    es_api_key: Optional[str] = Field(default=None, description="api key used for authentication")
    bearer_auth: Optional[str] = Field(
        default=None, description="bearer token used for HTTP bearer authentication"
    )
    ssl_assert_fingerprint: Optional[str] = Field(
        default=None, description="SHA256 fingerprint value"
    )


class ElasticsearchClientInput(BaseModel):
    hosts: Optional[list[str]] = None
    cloud_id: Optional[str] = None
    ca_certs: Optional[Path] = None
    basic_auth: Optional[Secret[tuple[str, str]]] = None
    api_key: Optional[Union[Secret[tuple[str, str]], SecretStr]] = None


class ElasticsearchConnectionConfig(ConnectionConfig):
    hosts: Optional[list[str]] = Field(
        default=None,
        description="list of the Elasticsearch hosts to connect to",
        examples=["http://localhost:9200"],
    )
    username: Optional[str] = Field(default=None, description="username when using basic auth")
    cloud_id: Optional[str] = Field(default=None, description="id used to connect to Elastic Cloud")
    api_key_id: Optional[str] = Field(
        default=None,
        description="id associated with api key used for authentication: "
        "https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html",  # noqa: E501
    )
    ca_certs: Optional[Path] = None
    access_config: Secret[ElasticsearchAccessConfig]

    def get_client_kwargs(self) -> dict:
        # Update auth related fields to conform to what the SDK expects based on the
        # supported methods:
        # https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/connecting.html
        client_input_kwargs: dict[str, Any] = {}
        access_config = self.access_config.get_secret_value()
        if self.hosts:
            client_input_kwargs["hosts"] = self.hosts
        if self.cloud_id:
            client_input_kwargs["cloud_id"] = self.cloud_id
        if self.ca_certs:
            client_input_kwargs["ca_certs"] = self.ca_certs
        if access_config.password and (
            self.cloud_id or self.ca_certs or access_config.ssl_assert_fingerprint
        ):
            client_input_kwargs["basic_auth"] = ("elastic", access_config.password)
        elif not self.cloud_id and self.username and access_config.password:
            client_input_kwargs["basic_auth"] = (self.username, access_config.password)
        elif access_config.es_api_key and self.api_key_id:
            client_input_kwargs["api_key"] = (self.api_key_id, access_config.es_api_key)
        elif access_config.es_api_key:
            client_input_kwargs["api_key"] = access_config.es_api_key
        client_input = ElasticsearchClientInput(**client_input_kwargs)
        logger.debug(f"Elasticsearch client inputs mapped to: {client_input.dict()}")
        client_kwargs = client_input.dict()
        client_kwargs["basic_auth"] = (
            client_input.basic_auth.get_secret_value() if client_input.basic_auth else None
        )
        client_kwargs["api_key"] = (
            client_input.api_key.get_secret_value() if client_input.api_key else None
        )
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}
        return client_kwargs

    @requires_dependencies(["elasticsearch"], extras="elasticsearch")
    def get_client(self) -> "ElasticsearchClient":
        from elasticsearch import Elasticsearch as ElasticsearchClient

        client = ElasticsearchClient(**self.get_client_kwargs())
        self.check_connection(client=client)
        return client

    def check_connection(self, client: "ElasticsearchClient"):
        try:
            client.perform_request("HEAD", "/", headers={"accept": "application/json"})
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")


class ElasticsearchIndexerConfig(IndexerConfig):
    index_name: str
    batch_size: int = 100


@dataclass
class ElasticsearchIndexer(Indexer):
    connection_config: ElasticsearchConnectionConfig
    index_config: ElasticsearchIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["elasticsearch"], extras="elasticsearch")
    def load_scan(self):
        from elasticsearch.helpers import scan

        return scan

    def _get_doc_ids(self) -> set[str]:
        """Fetches all document ids in an index"""
        scan = self.load_scan()

        scan_query: dict = {"stored_fields": [], "query": {"match_all": {}}}
        client = self.connection_config.get_client()
        hits = scan(
            client,
            query=scan_query,
            scroll="1m",
            index=self.index_config.index_name,
        )

        return {hit["_id"] for hit in hits}

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        all_ids = self._get_doc_ids()
        ids = list(all_ids)
        id_batches: list[frozenset[str]] = [
            frozenset(
                ids[
                    i
                    * self.index_config.batch_size : (i + 1)  # noqa
                    * self.index_config.batch_size
                ]
            )
            for i in range(
                (len(ids) + self.index_config.batch_size - 1) // self.index_config.batch_size
            )
        ]
        for batch in id_batches:
            # Make sure the hash is always a positive number to create identified
            identified = str(hash(batch) + sys.maxsize + 1)
            yield FileData(
                identifier=identified,
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    url=f"{self.connection_config.hosts[0]}/{self.index_config.index_name}",
                    date_processed=str(time()),
                ),
                additional_metadata={
                    "ids": list(batch),
                    "index_name": self.index_config.index_name,
                },
            )


class ElasticsearchDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


@dataclass
class ElasticsearchDownloader(Downloader):
    connection_config: ElasticsearchConnectionConfig
    download_config: ElasticsearchDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return True

    def get_identifier(self, index_name: str, record_id: str) -> str:
        f = f"{index_name}-{record_id}"
        if self.download_config.fields:
            f = "{}-{}".format(
                f,
                hashlib.sha256(",".join(self.download_config.fields).encode()).hexdigest()[:8],
            )
        return f

    def map_es_results(self, es_results: dict) -> str:
        doc_body = es_results["_source"]
        flattened_dict = flatten_dict(dictionary=doc_body)
        str_values = [str(value) for value in flattened_dict.values()]
        concatenated_values = "\n".join(str_values)
        return concatenated_values

    def generate_download_response(
        self, result: dict, index_name: str, file_data: FileData
    ) -> DownloadResponse:
        record_id = result["_id"]
        filename_id = self.get_identifier(index_name=index_name, record_id=record_id)
        filename = f"{filename_id}.txt"
        download_path = self.download_dir / Path(filename)
        logger.debug(
            f"Downloading results from index {index_name} and id {record_id} to {download_path}"
        )
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(download_path, "w", encoding="utf8") as f:
                f.write(self.map_es_results(es_results=result))
        except Exception as e:
            logger.error(
                f"failed to download from index {index_name} "
                f"and id {record_id} to {download_path}: {e}",
                exc_info=True,
            )
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        return DownloadResponse(
            file_data=FileData(
                identifier=filename_id,
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    version=str(result["_version"]) if "_version" in result else None,
                    date_processed=str(time()),
                    record_locator={
                        "hosts": self.connection_config.hosts,
                        "index_name": index_name,
                        "document_id": record_id,
                    },
                ),
            ),
            path=download_path,
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError()

    @requires_dependencies(["elasticsearch"], extras="elasticsearch")
    def load_async(self):
        from elasticsearch import AsyncElasticsearch
        from elasticsearch.helpers import async_scan

        return AsyncElasticsearch, async_scan

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        AsyncClient, async_scan = self.load_async()

        index_name: str = file_data.additional_metadata["index_name"]
        ids: list[str] = file_data.additional_metadata["ids"]

        scan_query = {
            "_source": self.download_config.fields,
            "version": True,
            "query": {"ids": {"values": ids}},
        }

        download_responses = []
        async with AsyncClient(**self.connection_config.get_client_kwargs()) as client:
            async for result in async_scan(
                client,
                query=scan_query,
                scroll="1m",
                index=index_name,
            ):
                download_responses.append(
                    self.generate_download_response(
                        result=result, index_name=index_name, file_data=file_data
                    )
                )
        return download_responses


class ElasticsearchUploadStagerConfig(UploadStagerConfig):
    index_name: str = Field(
        description="Name of the Elasticsearch index to pull data from, or upload data to."
    )


@dataclass
class ElasticsearchUploadStager(UploadStager):
    upload_stager_config: ElasticsearchUploadStagerConfig

    def conform_dict(self, data: dict) -> dict:
        resp = {
            "_index": self.upload_stager_config.index_name,
            "_id": str(uuid.uuid4()),
            "_source": {
                "element_id": data.pop("element_id", None),
                "embeddings": data.pop("embeddings", None),
                "text": data.pop("text", None),
                "type": data.pop("type", None),
            },
        }
        if "metadata" in data and isinstance(data["metadata"], dict):
            resp["_source"]["metadata"] = flatten_dict(data["metadata"], separator="-")
        return resp

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)
        conformed_elements = [self.conform_dict(data=element) for element in elements_contents]
        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(conformed_elements, output_file)
        return output_path


class ElasticsearchUploaderConfig(UploaderConfig):
    index_name: str = Field(
        description="Name of the Elasticsearch index to pull data from, or upload data to."
    )
    batch_size_bytes: int = Field(
        default=15_000_000,
        description="Size limit (in bytes) for each batch of items to be uploaded. Check"
        " https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html"
        "#_how_big_is_too_big for more information.",
    )
    num_threads: int = Field(
        default=4, description="Number of threads to be used while uploading content"
    )


@dataclass
class ElasticsearchUploader(Uploader):
    connector_type: str = CONNECTOR_TYPE
    upload_config: ElasticsearchUploaderConfig
    connection_config: ElasticsearchConnectionConfig

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["elasticsearch"], extras="elasticsearch")
    def load_parallel_bulk(self):
        from elasticsearch.helpers import parallel_bulk

        return parallel_bulk

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        parallel_bulk = self.load_parallel_bulk()
        with path.open("r") as file:
            elements_dict = json.load(file)
        upload_destination = self.connection_config.hosts or self.connection_config.cloud_id

        logger.info(
            f"writing {len(elements_dict)} elements via document batches to destination "
            f"index named {self.upload_config.index_name} at {upload_destination} with "
            f"batch size (in bytes) {self.upload_config.batch_size_bytes} with "
            f"{self.upload_config.num_threads} (number of) threads"
        )

        client = self.connection_config.get_client()
        if not client.indices.exists(index=self.upload_config.index_name):
            logger.warning(
                f"{(self.__class__.__name__).replace('Uploader', '')} index does not exist: "
                f"{self.upload_config.index_name}. "
                f"This may cause issues when uploading."
            )
        for batch in generator_batching_wbytes(
            elements_dict, batch_size_limit_bytes=self.upload_config.batch_size_bytes
        ):
            for success, info in parallel_bulk(
                client=client,
                actions=batch,
                thread_count=self.upload_config.num_threads,
            ):
                if not success:
                    logger.error(
                        "upload failed for a batch in "
                        f"{(self.__class__.__name__).replace('Uploader', '')} "
                        "destination connector:",
                        info,
                    )


elasticsearch_source_entry = SourceRegistryEntry(
    connection_config=ElasticsearchConnectionConfig,
    indexer=ElasticsearchIndexer,
    indexer_config=ElasticsearchIndexerConfig,
    downloader=ElasticsearchDownloader,
    downloader_config=ElasticsearchDownloaderConfig,
)

elasticsearch_destination_entry = DestinationRegistryEntry(
    connection_config=ElasticsearchConnectionConfig,
    upload_stager_config=ElasticsearchUploadStagerConfig,
    upload_stager=ElasticsearchUploadStager,
    uploader_config=ElasticsearchUploaderConfig,
    uploader=ElasticsearchUploader,
)
