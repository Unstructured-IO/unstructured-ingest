import json
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from unstructured_ingest.enhanced_dataclass import enhanced_field
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploadContent,
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
    from couchbase.cluster import Cluster

CONNECTOR_TYPE = "couchbase"
SERVER_API_VERSION = "1"


@dataclass
class CouchbaseAccessConfig(AccessConfig):
    password: str


@dataclass
class CouchbaseConnectionConfig(ConnectionConfig):
    connection_string: str
    username: str
    bucket: str
    scope: str
    collection: str
    access_config: CouchbaseAccessConfig = enhanced_field(default_factory=CouchbaseAccessConfig, sensitive=True)
    batch_size: int = 50
    connector_type: str = CONNECTOR_TYPE


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
    upload_config: CouchbaseUploaderConfig
    connection_config: CouchbaseConnectionConfig
    cluster: Optional["Cluster"] = field(init=False, default=None)
    connector_type: str = CONNECTOR_TYPE

    def __post_init__(self):
        try:
            self.cluster = self.connect_to_couchbase()
        except Exception as e:
            logger.error(f"Error connecting to couchbase: {e}")

    @requires_dependencies(["couchbase"], extras="couchbase")
    def connect_to_couchbase(self) -> "Cluster":
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        access_conf = self.connection_config.access_config
        connection_string = access_conf.connection_string
        username = access_conf.username
        password = access_conf.password

        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        return cluster

    def run(self, contents: list[UploadContent], **kwargs: Any) -> None:
        elements = []
        for content in contents:
            with open(content.path) as elements_file:
                elements.extend(json.load(elements_file))

        logger.info(
            f"writing {len(elements)} objects to destination "
            f"bucket, {self.connection_config.bucket} "
            f"at {self.connection_config.access_config.connection_string}",
        )
        bucket = self.cluster.bucket(self.connection_config.bucket)
        scope = bucket.scope(self.connection_config.scope)
        collection = scope.collection(self.connection_config.collection)

        for chunk in batch_generator(elements, self.upload_config.batch_size):
            collection.upsert_multi({doc_id: doc for doc in chunk for doc_id, doc in doc.items()})


couchbase_destination_entry = DestinationRegistryEntry(
    connection_config=CouchbaseConnectionConfig,
    uploader=CouchbaseUploader,
    uploader_config=CouchbaseUploaderConfig,
    upload_stager=CouchbaseUploadStager,
    upload_stager_config=CouchbaseUploadStagerConfig,
)
