import asyncio
import hashlib
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, List, Optional

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.error import (
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
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
    UploadContent,
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
    from couchbase.cluster import Cluster

CONNECTOR_TYPE = "couchbase"
SERVER_API_VERSION = "1"


@requires_dependencies(["couchbase"], extras="couchbase")
def connect_to_couchbase(connection_string: str, username: str, password: str):
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions

    auth = PasswordAuthenticator(username, password)
    options = ClusterOptions(auth)
    options.apply_profile("wan_development")
    cluster = Cluster(connection_string, options)
    cluster.wait_until_ready(timedelta(seconds=5))
    return cluster


@dataclass
class CouchbaseAccessConfig(AccessConfig):
    password: str


@dataclass
class CouchbaseConnectionConfig(ConnectionConfig):
    username: str
    bucket: str
    connection_string: str = "couchbase://localhost"
    scope: str = "_default"
    collection: str = "_default"
    batch_size: int = 50
    connector_type: str = CONNECTOR_TYPE
    access_config: CouchbaseAccessConfig = enhanced_field(sensitive=True)


@dataclass
class CouchbaseUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class CouchbaseUploadStager(UploadStager):
    upload_stager_config: CouchbaseUploadStagerConfig = field(
        default_factory=lambda: CouchbaseUploadStagerConfig()
    )

    def run(
        self,
        elements_filepath: Path,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        output_elements = []
        for element in elements_contents:
            new_doc = {
                element["element_id"]: {
                    "embedding": element.get("embeddings", None),
                    "text": element.get("text", None),
                    "metadata": element.get("metadata", None),
                    "type": element.get("type", None),
                }
            }
            output_elements.append(new_doc)

        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(output_elements, output_file)
        return output_path


@dataclass
class CouchbaseUploaderConfig(UploaderConfig):
    batch_size: int = 50


@dataclass
class CouchbaseUploader(Uploader):
    connection_config: CouchbaseConnectionConfig
    upload_config: CouchbaseUploaderConfig
    cluster: Optional["Cluster"] = field(init=False, default=None)
    connector_type: str = CONNECTOR_TYPE

    def __post_init__(self):
        self.cluster = self.get_cluster()

    @requires_dependencies(["couchbase"], extras="couchbase")
    def get_cluster(self) -> "Cluster":
        return connect_to_couchbase(
            self.connection_config.connection_string,
            self.connection_config.username,
            self.connection_config.access_config.password,
        )

    def run(self, contents: list[UploadContent], **kwargs: Any) -> None:
        elements = []
        for content in contents:
            with open(content.path) as elements_file:
                elements.extend(json.load(elements_file))

        logger.info(
            f"writing {len(elements)} objects to destination "
            f"bucket, {self.connection_config.bucket} "
            f"at {self.connection_config.connection_string}",
        )
        bucket = self.cluster.bucket(self.connection_config.bucket)
        scope = bucket.scope(self.connection_config.scope)
        collection = scope.collection(self.connection_config.collection)

        for chunk in batch_generator(elements, self.upload_config.batch_size):
            collection.upsert_multi({doc_id: doc for doc in chunk for doc_id, doc in doc.items()})


@dataclass
class CouchbaseIndexerConfig(IndexerConfig):
    batch_size: int = 100


@dataclass
class CouchbaseIndexer(Indexer):
    connection_config: CouchbaseConnectionConfig
    index_config: CouchbaseIndexerConfig
    connector_type: str = CONNECTOR_TYPE
    cluster: Optional["Cluster"] = field(init=False, default=None)

    def __post_init__(self):
        try:
            self.cluster = self.get_cluster()
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    @requires_dependencies(["couchbase"], extras="couchbase")
    def get_cluster(self) -> "Cluster":
        return connect_to_couchbase(
            self.connection_config.connection_string,
            self.connection_config.username,
            self.connection_config.access_config.password,
        )

    @requires_dependencies(["couchbase"], extras="couchbase")
    def _get_doc_ids(self) -> List[str]:
        query = (
            f"SELECT META(d).id "
            f"FROM `{self.connection_config.bucket}`."
            f"`{self.connection_config.scope}`."
            f"`{self.connection_config.collection}` as d"
        )

        max_attempts = 5
        attempts = 0
        while attempts < max_attempts:
            try:
                result = self.cluster.query(query)
                document_ids = [row["id"] for row in result]
                return document_ids
            except Exception as e:
                attempts += 1
                time.sleep(3)
                if attempts == max_attempts:
                    raise SourceConnectionError(f"failed to get document ids: {e}")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        ids = self._get_doc_ids()

        id_batches = [
            ids[i * self.connection_config.batch_size : (i + 1) * self.connection_config.batch_size]
            for i in range(
                (len(ids) + self.connection_config.batch_size - 1)
                // self.connection_config.batch_size
            )
        ]
        for batch in id_batches:
            # Make sure the hash is always a positive number to create identified
            identified = str(hash(tuple(batch)) + sys.maxsize + 1)
            yield FileData(
                identifier=identified,
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    url=f"{self.connection_config.connection_string}/"
                    f"{self.connection_config.bucket}",
                    date_processed=str(time.time()),
                ),
                additional_metadata={
                    "ids": list(batch),
                    "bucket": self.connection_config.bucket,
                },
            )


@dataclass
class CouchbaseDownloaderConfig(DownloaderConfig):
    fields: list[str] = field(default_factory=list)


@dataclass
class CouchbaseDownloader(Downloader):
    connection_config: CouchbaseConnectionConfig
    download_config: CouchbaseDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["couchbase"], extras="couchbase")
    def get_cluster(self) -> "Cluster":
        return connect_to_couchbase(
            self.connection_config.connection_string,
            self.connection_config.username,
            self.connection_config.access_config.password,
        )

    def is_async(self) -> bool:
        return True

    def get_identifier(self, bucket: str, record_id: str) -> str:
        f = f"{bucket}-{record_id}"
        if self.download_config.fields:
            f = "{}-{}".format(
                f,
                hashlib.sha256(",".join(self.download_config.fields).encode()).hexdigest()[:8],
            )
        return f

    def map_cb_results(self, cb_results: dict) -> str:
        doc_body = cb_results
        flattened_dict = flatten_dict(dictionary=doc_body)
        str_values = [str(value) for value in flattened_dict.values()]
        concatenated_values = "\n".join(str_values)
        return concatenated_values

    def generate_download_response(
        self, result: dict, bucket: str, file_data: FileData
    ) -> DownloadResponse:
        record_id = result["id"]
        filename_id = self.get_identifier(bucket=bucket, record_id=record_id)
        filename = f"{filename_id}.txt"
        download_path = self.download_dir / Path(filename)
        logger.debug(
            f"Downloading results from bucket {bucket} and id {record_id} to {download_path}"
        )
        download_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(download_path, "w", encoding="utf8") as f:
                f.write(self.map_cb_results(cb_results=result))
        except Exception as e:
            logger.error(
                f"failed to download from bucket {bucket} "
                f"and id {record_id} to {download_path}: {e}",
                exc_info=True,
            )
            raise SourceConnectionNetworkError(f"failed to download file {file_data.identifier}")
        return DownloadResponse(
            file_data=FileData(
                identifier=filename_id,
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    version=None,
                    date_processed=str(time.time()),
                    record_locator={
                        "connection_string": self.connection_config.connection_string,
                        "bucket": bucket,
                        "scope": self.connection_config.scope,
                        "collection": self.connection_config.collection,
                        "document_id": record_id,
                    },
                ),
            ),
            path=download_path,
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError()

    async def process_doc_id(self, doc_id, collection, bucket_name, file_data):
        result = collection.get(doc_id)
        return self.generate_download_response(
            result=result.content_as[dict], bucket=bucket_name, file_data=file_data
        )

    async def process_all_doc_ids(self, ids, collection, bucket_name, file_data):
        asyncio.get_event_loop()
        with ThreadPoolExecutor():
            tasks = [
                self.process_doc_id(doc_id, collection, bucket_name, file_data) for doc_id in ids
            ]
            return await asyncio.gather(*tasks)

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:

        bucket_name: str = file_data.additional_metadata["bucket"]
        ids: list[str] = file_data.additional_metadata["ids"]

        cluster = self.get_cluster()
        bucket = cluster.bucket(bucket_name)
        scope = bucket.scope(self.connection_config.scope)
        collection = scope.collection(self.connection_config.collection)

        download_resp = await self.process_all_doc_ids(ids, collection, bucket_name, file_data)
        return list(download_resp)


couchbase_destination_entry = DestinationRegistryEntry(
    connection_config=CouchbaseConnectionConfig,
    uploader=CouchbaseUploader,
    uploader_config=CouchbaseUploaderConfig,
    upload_stager=CouchbaseUploadStager,
    upload_stager_config=CouchbaseUploadStagerConfig,
)

couchbase_source_entry = SourceRegistryEntry(
    connection_config=CouchbaseConnectionConfig,
    indexer=CouchbaseIndexer,
    indexer_config=CouchbaseIndexerConfig,
    downloader=CouchbaseDownloader,
    downloader_config=CouchbaseDownloaderConfig,
)
