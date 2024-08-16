import json
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError
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


class CouchbaseUploaderConfig(UploaderConfig):
    batch_size: int = Field(default=50, description="Number of documents to upload per batch")


@dataclass
class CouchbaseUploader(Uploader):
    connection_config: CouchbaseConnectionConfig
    upload_config: CouchbaseUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["couchbase"], extras="couchbase")
    def connect_to_couchbase(self) -> "Cluster":
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.options import ClusterOptions

        connection_string = self.connection_config.connection_string
        username = self.connection_config.username
        password = self.connection_config.access_config.get_secret_value().password

        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)
        options.apply_profile("wan_development")
        cluster = Cluster(connection_string, options)
        cluster.wait_until_ready(timedelta(seconds=5))
        return cluster

    def precheck(self) -> None:
        try:
            self.connect_to_couchbase()
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

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
        cluster = self.connect_to_couchbase()
        bucket = cluster.bucket(self.connection_config.bucket)
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
