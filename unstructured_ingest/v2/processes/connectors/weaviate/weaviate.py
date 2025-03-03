import json
import re
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.error import DestinationConnectionError, WriteError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.constants import RECORD_ID_LABEL
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    FileData,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    VectorDBUploader,
)
from unstructured_ingest.v2.logger import logger

if TYPE_CHECKING:
    from weaviate.classes.init import Timeout
    from weaviate.client import WeaviateClient
    from weaviate.collections.batch.client import BatchClient

CONNECTOR_TYPE = "weaviate"


class WeaviateAccessConfig(AccessConfig, ABC):
    pass


class WeaviateConnectionConfig(ConnectionConfig, ABC):
    init_timeout: int = Field(default=2, ge=0, description="Timeout for initialization checks")
    insert_timeout: int = Field(default=90, ge=0, description="Timeout for insert operations")
    query_timeout: int = Field(default=30, ge=0, description="Timeout for query operations")
    access_config: Secret[WeaviateAccessConfig] = Field(
        default=WeaviateAccessConfig(), validate_default=True
    )

    @requires_dependencies(["weaviate"], extras="weaviate")
    def get_timeout(self) -> "Timeout":
        from weaviate.classes.init import Timeout

        return Timeout(init=self.init_timeout, query=self.query_timeout, insert=self.insert_timeout)

    @abstractmethod
    @contextmanager
    def get_client(self) -> Generator["WeaviateClient", None, None]:
        pass


class WeaviateUploadStagerConfig(UploadStagerConfig):
    pass


@dataclass
class WeaviateUploadStager(UploadStager):
    upload_stager_config: WeaviateUploadStagerConfig = field(
        default_factory=lambda: WeaviateUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> date:
        try:
            timestamp = float(date_string)
            return datetime.fromtimestamp(timestamp)
        except Exception as e:
            logger.debug(f"date {date_string} string not a timestamp: {e}")
        return parser.parse(date_string)

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        """
        Updates the element dictionary to conform to the Weaviate schema
        """
        data = element_dict.copy()
        working_data = data.copy()
        # Dict as string formatting
        if (
            record_locator := working_data.get("metadata", {})
            .get("data_source", {})
            .get("record_locator")
        ):
            # Explicit casting otherwise fails schema type checking
            working_data["metadata"]["data_source"]["record_locator"] = str(
                json.dumps(record_locator)
            )

        # Array of items as string formatting
        if points := working_data.get("metadata", {}).get("coordinates", {}).get("points"):
            working_data["metadata"]["coordinates"]["points"] = str(json.dumps(points))

        if links := working_data.get("metadata", {}).get("links", {}):
            working_data["metadata"]["links"] = str(json.dumps(links))

        if permissions_data := (
            working_data.get("metadata", {}).get("data_source", {}).get("permissions_data")
        ):
            working_data["metadata"]["data_source"]["permissions_data"] = json.dumps(
                permissions_data
            )

        # Datetime formatting
        if (
            date_created := working_data.get("metadata", {})
            .get("data_source", {})
            .get("date_created")
        ):
            working_data["metadata"]["data_source"]["date_created"] = self.parse_date_string(
                date_created
            ).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )

        if (
            date_modified := working_data.get("metadata", {})
            .get("data_source", {})
            .get("date_modified")
        ):
            working_data["metadata"]["data_source"]["date_modified"] = self.parse_date_string(
                date_modified
            ).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )

        if (
            date_processed := working_data.get("metadata", {})
            .get("data_source", {})
            .get("date_processed")
        ):
            working_data["metadata"]["data_source"]["date_processed"] = self.parse_date_string(
                date_processed
            ).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )

        if last_modified := working_data.get("metadata", {}).get("last_modified"):
            working_data["metadata"]["last_modified"] = self.parse_date_string(
                last_modified
            ).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ",
            )

        # String casting
        if version := working_data.get("metadata", {}).get("data_source", {}).get("version"):
            working_data["metadata"]["data_source"]["version"] = str(version)

        if page_number := working_data.get("metadata", {}).get("page_number"):
            working_data["metadata"]["page_number"] = str(page_number)

        if regex_metadata := working_data.get("metadata", {}).get("regex_metadata"):
            working_data["metadata"]["regex_metadata"] = str(json.dumps(regex_metadata))

        working_data[RECORD_ID_LABEL] = file_data.identifier
        return working_data


class WeaviateUploaderConfig(UploaderConfig):
    collection: Optional[str] = Field(
        description="The name of the collection this object belongs to", default=None
    )
    batch_size: Optional[int] = Field(default=None, description="Number of records per batch")
    requests_per_minute: Optional[int] = Field(default=None, description="Rate limit for upload")
    dynamic_batch: bool = Field(default=True, description="Whether to use dynamic batch")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )

    def model_post_init(self, __context: Any) -> None:
        batch_types = {
            "fixed_size": self.batch_size is not None,
            "rate_limited": self.requests_per_minute is not None,
            "dynamic": self.dynamic_batch,
        }

        enabled_batch_modes = [batch_key for batch_key, flag in batch_types.items() if flag]
        if not enabled_batch_modes:
            raise ValueError("No batch mode enabled")
        if len(enabled_batch_modes) > 1:
            raise ValueError(
                "Multiple batch modes enabled, only one mode can be used: {}".format(
                    ", ".join(enabled_batch_modes)
                )
            )
        logger.info(f"Uploader config instantiated with {enabled_batch_modes[0]} batch mode")

    @contextmanager
    def get_batch_client(self, client: "WeaviateClient") -> Generator["BatchClient", None, None]:
        if self.dynamic_batch:
            with client.batch.dynamic() as batch_client:
                yield batch_client
        elif self.batch_size:
            with client.batch.fixed_size(batch_size=self.batch_size) as batch_client:
                yield batch_client
        elif self.requests_per_minute:
            with client.batch.rate_limit(
                requests_per_minute=self.requests_per_minute
            ) as batch_client:
                yield batch_client
        else:
            raise ValueError("No batch mode enabled")


@dataclass
class WeaviateUploader(VectorDBUploader, ABC):
    upload_config: WeaviateUploaderConfig
    connection_config: WeaviateConnectionConfig

    def _collection_exists(self, collection_name: Optional[str] = None):
        collection_name = collection_name or self.upload_config.collection
        with self.connection_config.get_client() as weaviate_client:
            return weaviate_client.collections.exists(name=collection_name)

    def precheck(self) -> None:
        try:
            self.connection_config.get_client()
            # only if collection name populated should we check that it exists
            if self.upload_config.collection and not self._collection_exists():
                raise DestinationConnectionError(
                    f"collection '{self.upload_config.collection}' does not exist"
                )
        except Exception as e:
            logger.error(f"Failed to validate connection {e}", exc_info=True)
            raise DestinationConnectionError(f"failed to validate connection: {e}")

    def init(self, **kwargs: Any) -> None:
        self.create_destination(**kwargs)

    def format_destination_name(self, destination_name: str) -> str:
        # Weaviate naming requirements:
        # must be alphanumeric and underscores only
        formatted = re.sub(r"[^a-zA-Z0-9]", "_", destination_name)
        # must begin with capital letter
        return formatted.capitalize()

    def create_destination(
        self,
        destination_name: str = "unstructuredautocreated",
        vector_length: Optional[int] = None,
        **kwargs: Any,
    ) -> bool:
        collection_name = self.upload_config.collection or destination_name
        collection_name = self.format_destination_name(collection_name)
        self.upload_config.collection = collection_name

        connectors_dir = Path(__file__).parents[1]
        collection_config_file = connectors_dir / "assets" / "weaviate_collection_config.json"
        with collection_config_file.open() as f:
            collection_config = json.load(f)
        collection_config["class"] = collection_name

        if not self._collection_exists():
            logger.info(f"creating weaviate collection '{collection_name}' with default configs")
            with self.connection_config.get_client() as weaviate_client:
                weaviate_client.collections.create_from_dict(config=collection_config)
                return True
        logger.debug(f"collection with name '{collection_name}' already exists, skipping creation")
        return False

    def check_for_errors(self, client: "WeaviateClient") -> None:
        failed_uploads = client.batch.failed_objects
        if failed_uploads:
            for failure in failed_uploads:
                logger.error(
                    f"Failed to upload object with id {failure.original_uuid}: {failure.message}"
                )
            raise WriteError("Failed to upload to weaviate")

    @requires_dependencies(["weaviate"], extras="weaviate")
    def delete_by_record_id(self, client: "WeaviateClient", file_data: FileData) -> None:
        from weaviate.classes.query import Filter

        record_id = file_data.identifier
        collection = client.collections.get(self.upload_config.collection)
        delete_filter = Filter.by_property(name=self.upload_config.record_id_key).equal(
            val=record_id
        )
        # There is a configurable maximum limit (QUERY_MAXIMUM_RESULTS) on the number of
        # objects that can be deleted in a single query (default 10,000). To delete
        # more objects than the limit, re-run the query until nothing is deleted.
        while True:
            resp = collection.data.delete_many(where=delete_filter)
            if resp.failed:
                raise WriteError(
                    f"failed to delete records in collection "
                    f"{self.upload_config.collection} with record "
                    f"id property {record_id}"
                )
            if not resp.failed and not resp.successful:
                break

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        logger.info(
            f"writing {len(data)} objects to destination "
            f"class {self.connection_config.access_config} "
        )
        if not self.upload_config.collection:
            raise ValueError("No collection specified")

        with self.connection_config.get_client() as weaviate_client:
            self.delete_by_record_id(client=weaviate_client, file_data=file_data)
            with self.upload_config.get_batch_client(client=weaviate_client) as batch_client:
                for e in data:
                    vector = e.pop("embeddings", None)
                    batch_client.add_object(
                        collection=self.upload_config.collection,
                        properties=e,
                        vector=vector,
                    )
            self.check_for_errors(client=weaviate_client)
