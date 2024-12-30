import hashlib
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, List

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.error import (
    DestinationConnectionError,
    SourceConnectionError,
    SourceConnectionNetworkError,
)
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    BatchFileData,
    BatchItem,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
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
    from couchbase.collection import Collection

CONNECTOR_TYPE = "couchbase"
SERVER_API_VERSION = "1"


class CouchbaseAdditionalMetadata(BaseModel):
    bucket: str


class CouchbaseBatchFileData(BatchFileData):
    additional_metadata: CouchbaseAdditionalMetadata


class CouchbaseAccessConfig(AccessConfig):
    password: str = Field(description="The password for the Couchbase server")


class CouchbaseConnectionConfig(ConnectionConfig):
    username: str = Field(description="The username for the Couchbase server")
    bucket: str = Field(description="The bucket to connect to on the Couchbase server")
    connection_string: str = Field(
        default="couchbase://localhost", description="The connection string of the Couchbase server"
    )
    scope: str = Field(
        default="_default", description="The scope to connect to on the Couchbase server"
    )
    collection: str = Field(
        default="_default", description="The collection to connect to on the Couchbase server"
    )
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    access_config: Secret[CouchbaseAccessConfig]

    @requires_dependencies(["couchbase"], extras="couchbase")
    @contextmanager
    def get_client(self) -> Generator["Cluster", None, None]:
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        auth = PasswordAuthenticator(self.username, self.access_config.get_secret_value().password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = None
        try:
            cluster = Cluster(self.connection_string, options)
            cluster.wait_until_ready(timedelta(seconds=5))
            yield cluster
        finally:
            if cluster:
                cluster.close()


class CouchbaseUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class CouchbaseUploadStager(UploadStager):
    upload_stager_config: CouchbaseUploadStagerConfig = field(
        default_factory=lambda: CouchbaseUploadStagerConfig()
    )

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        data = element_dict.copy()
        return {
            data["element_id"]: {
                "embedding": data.get("embeddings", None),
                "text": data.get("text", None),
                "metadata": data.get("metadata", None),
                "type": data.get("type", None),
            }
        }


class CouchbaseUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of documents to upload per batch")


@dataclass
class CouchbaseUploader(Uploader):
    connection_config: CouchbaseConnectionConfig
    upload_config: CouchbaseUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        logger.info(
            f"writing {len(data)} objects to destination "
            f"bucket, {self.connection_config.bucket} "
            f"at {self.connection_config.connection_string}",
        )
        with self.connection_config.get_client() as client:
            bucket = client.bucket(self.connection_config.bucket)
            scope = bucket.scope(self.connection_config.scope)
            collection = scope.collection(self.connection_config.collection)

            for chunk in batch_generator(data, self.upload_config.batch_size):
                collection.upsert_multi(
                    {doc_id: doc for doc in chunk for doc_id, doc in doc.items()}
                )


class CouchbaseIndexerConfig(IndexerConfig):
    batch_size: int = Field(default=50, description="Number of documents to index per batch")


@dataclass
class CouchbaseIndexer(Indexer):
    connection_config: CouchbaseConnectionConfig
    index_config: CouchbaseIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

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
                with self.connection_config.get_client() as client:
                    result = client.query(query)
                    document_ids = [row["id"] for row in result]
                    return document_ids
            except Exception as e:
                attempts += 1
                time.sleep(3)
                if attempts == max_attempts:
                    raise SourceConnectionError(f"failed to get document ids: {e}")

    def run(self, **kwargs: Any) -> Generator[CouchbaseBatchFileData, None, None]:
        ids = self._get_doc_ids()
        for batch in batch_generator(ids, self.index_config.batch_size):
            # Make sure the hash is always a positive number to create identified
            yield CouchbaseBatchFileData(
                connector_type=CONNECTOR_TYPE,
                metadata=FileDataSourceMetadata(
                    url=f"{self.connection_config.connection_string}/"
                    f"{self.connection_config.bucket}",
                    date_processed=str(time.time()),
                ),
                additional_metadata=CouchbaseAdditionalMetadata(
                    bucket=self.connection_config.bucket
                ),
                batch_items=[BatchItem(identifier=b) for b in batch],
            )


class CouchbaseDownloaderConfig(DownloaderConfig):
    collection_id: str = Field(
        default="id", description="The unique key of the id field in the collection"
    )
    fields: list[str] = field(default_factory=list)


@dataclass
class CouchbaseDownloader(Downloader):
    connection_config: CouchbaseConnectionConfig
    download_config: CouchbaseDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return False

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
        self, result: dict, bucket: str, file_data: CouchbaseBatchFileData
    ) -> DownloadResponse:
        record_id = result[self.download_config.collection_id]
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
        file_data.source_identifiers = SourceIdentifiers(filename=filename, fullpath=filename)
        cast_file_data = FileData.cast(file_data=file_data)
        cast_file_data.identifier = filename_id
        cast_file_data.metadata.date_processed = str(time.time())
        cast_file_data.metadata.record_locator = {
            "connection_string": self.connection_config.connection_string,
            "bucket": bucket,
            "scope": self.connection_config.scope,
            "collection": self.connection_config.collection,
            "document_id": record_id,
        }
        return super().generate_download_response(
            file_data=cast_file_data,
            download_path=download_path,
        )

    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        couchbase_file_data = CouchbaseBatchFileData.cast(file_data=file_data)
        bucket_name: str = couchbase_file_data.additional_metadata.bucket
        ids: list[str] = [item.identifier for item in couchbase_file_data.batch_items]

        with self.connection_config.get_client() as client:
            bucket = client.bucket(bucket_name)
            scope = bucket.scope(self.connection_config.scope)
            collection = scope.collection(self.connection_config.collection)

            download_resp = self.process_all_doc_ids(ids, collection, bucket_name, file_data)
            return list(download_resp)

    def process_doc_id(
        self,
        doc_id: str,
        collection: "Collection",
        bucket_name: str,
        file_data: CouchbaseBatchFileData,
    ):
        result = collection.get(doc_id)
        return self.generate_download_response(
            result=result.content_as[dict], bucket=bucket_name, file_data=file_data
        )

    def process_all_doc_ids(
        self,
        ids: list[str],
        collection: "Collection",
        bucket_name: str,
        file_data: CouchbaseBatchFileData,
    ):
        for doc_id in ids:
            yield self.process_doc_id(doc_id, collection, bucket_name, file_data)

    async def run_async(self, file_data: FileData, **kwargs: Any) -> download_responses:
        raise NotImplementedError()


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
