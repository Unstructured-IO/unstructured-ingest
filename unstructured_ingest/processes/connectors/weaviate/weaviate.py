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

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import (
    DestinationConnectionError,
    ValueError,
    WriteError,
    safe_error_summary,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    VectorDBUploader,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies

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
    flatten_metadata: bool = Field(
        default=False,
        description=(
            "Flatten nested metadata into top-level properties "
            "(e.g. metadata.data_source.version -> data_source_version). "
            "Requires pre-existing collection if auto_schema is disabled."
        ),
    )
    auto_schema: bool = Field(
        default=False,
        description=(
            "Rely on Weaviate's auto-schema to build the collection and its "
            "properties from the uploaded objects. When true, the collection "
            "and its properties are created automatically. "
            "When false, the collection must already exist and each "
            "object is conformed to it (unknown properties are dropped, missing "
            "ones set to null). Requires AUTOSCHEMA_ENABLED=true in Weaviate."
        ),
    )


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

        if self.upload_stager_config.flatten_metadata:
            if self.upload_stager_config.auto_schema:
                # auto_schema: Weaviate infers each column's type from the values.
                # Apply normalization as the non-flatten path.
                self._conform_metadata_values(working_data)
            # else: pure flatten — the user owns the schema and declares types
            # matching the raw values (e.g. OBJECT_ARRAY for list[dict] fields
            # like links, permissions_data, regex_metadata_<pattern>).
            metadata = working_data.pop("metadata", {})
            working_data.update(
                flatten_dict(
                    metadata,
                    separator="_",
                    flatten_lists=False,
                    remove_none=True,
                )
            )
            working_data[RECORD_ID_LABEL] = file_data.identifier
            return working_data

        self._conform_metadata_values(working_data)
        working_data[RECORD_ID_LABEL] = file_data.identifier
        return working_data

    def _conform_metadata_values(self, working_data: dict) -> None:
        """Normalize nested metadata values into Weaviate-typeable forms in place:
        stringify dicts / 2-D arrays / list[dict] fields that have no native scalar
        type, format dates as RFC3339, and cast version/page_number to strings."""
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


class WeaviateUploaderConfig(UploaderConfig):
    collection: Optional[str] = Field(
        description=(
            "The name of the collection this object belongs to. "
            "If not provided, a default collection name will be used."
        ),
        default=None,
    )
    batch_size: Optional[int] = Field(default=None, description="Number of records per batch")
    requests_per_minute: Optional[int] = Field(default=None, description="Rate limit for upload")
    dynamic_batch: bool = Field(default=True, description="Whether to use dynamic batch")
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )
    flatten_metadata: bool = Field(
        default=False,
        description=(
            "Flatten nested metadata into top-level properties "
            "(e.g. metadata.data_source.version -> data_source_version). "
            "Requires pre-existing collection if auto_schema is disabled."
        ),
    )
    auto_schema: bool = Field(
        default=False,
        description=(
            "Rely on Weaviate's auto-schema to build the collection and its "
            "properties from the uploaded objects. When true, the collection "
            "and its properties are created automatically. "
            "When false, the collection must already exist and each "
            "object is conformed to it (unknown properties are dropped, missing "
            "ones set to null). Requires AUTOSCHEMA_ENABLED=true in Weaviate."
        ),
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
    _schema_property_names: Optional[set[str]] = field(init=False, repr=False, default=None)
    _collection_exists: Optional[bool] = field(init=False, repr=False, default=None)

    def _collection_present(self, client: "WeaviateClient") -> bool:
        """Whether the destination collection exists, memoized across a run.

        The collection name is fixed for an uploader and, once the collection exists,
        it will not disappear mid-run, so a positive result is cached to avoid
        repeating the existence round-trip on every upload. A negative result is not
        cached: in auto_schema mode Weaviate creates the collection on the first
        insert, so it can flip from absent to present within the same run.
        """
        if not self._collection_exists:
            self._collection_exists = client.collections.exists(
                name=self.upload_config.collection
            )
        return self._collection_exists

    def get_schema_property_names(self, client: "WeaviateClient") -> set[str]:
        if self._schema_property_names is None:
            collection = client.collections.get(self.upload_config.collection)
            config = collection.config.get()
            self._schema_property_names = {p.name for p in config.properties}
        return self._schema_property_names

    @staticmethod
    def conform_to_schema(properties: dict, schema_props: set[str]) -> dict:
        """Drop unknown keys; fill missing schema keys with None for explicit-null queries."""
        return {k: properties.get(k) for k in schema_props}

    def precheck(self) -> None:
        try:
            with self.connection_config.get_client() as weaviate_client:
                if not weaviate_client.is_connected():
                    raise DestinationConnectionError("failed to connect to Weaviate")

                if self.upload_config.auto_schema:
                    # Weaviate creates the collection and its properties on the
                    # first insert (requires AUTOSCHEMA_ENABLED=true)
                    return

                if self.upload_config.flatten_metadata and not self.upload_config.collection:
                    raise DestinationConnectionError(
                        "flatten_metadata=true requires an explicit collection name "
                        "when auto_schema is disabled."
                    )

                if not self.upload_config.collection:
                    return

                if not self._collection_present(weaviate_client):
                    raise DestinationConnectionError(
                        f"Collection '{self.upload_config.collection}' does not exist "
                        "(must be pre-created when auto_schema is disabled)"
                    )

                if self.upload_config.flatten_metadata:
                    # Schema shape (OBJECT_ARRAY for nested fields, etc.) is the
                    # user's call; we only enforce that record_id is declared so
                    # re-run delete-by-record_id can find prior writes.
                    collection = weaviate_client.collections.get(self.upload_config.collection)
                    schema_props = {p.name for p in collection.config.get().properties}
                    if self.upload_config.record_id_key not in schema_props:
                        raise DestinationConnectionError(
                            f"Collection {self.upload_config.collection!r} is missing "
                            f"required property {self.upload_config.record_id_key!r}; "
                            "delete-by-record_id will not work."
                        )
        except DestinationConnectionError:
            raise
        except Exception as e:
            logger.error(f"Failed to validate connection: {safe_error_summary(e)}")
            raise DestinationConnectionError(
                f"failed to validate connection: {safe_error_summary(e)}"
            ) from None

    def init(self, **kwargs: Any) -> None:
        self.create_destination(**kwargs)

    def format_destination_name(self, destination_name: str) -> str:
        """
        Weaviate Collection naming conventions:
            1. must begin with an uppercase letter
            2. must be alphanumeric and underscores only
        """

        # Check if the first character is an uppercase letter
        if not re.match(r"^[a-zA-Z]", destination_name):
            raise ValueError("Collection name must start with an uppercase letter")
        # Replace all non-alphanumeric characters with underscores
        formatted = re.sub(r"[^a-zA-Z0-9]", "_", destination_name)
        # Make the first character uppercase and leave the rest as is
        if len(formatted) == 1:
            formatted = formatted.capitalize()
        else:
            formatted = formatted[0].capitalize() + formatted[1:]
        if formatted != destination_name:
            logger.warning(
                f"Given Collection name '{destination_name}' doesn't follow naming conventions. "
                f"Renaming to '{formatted}'"
            )
        return formatted

    def create_destination(
        self,
        destination_name: str = "Unstructuredautocreated",
        vector_length: Optional[int] = None,
        **kwargs: Any,
    ) -> bool:
        collection_name = self.upload_config.collection or destination_name
        collection_name = self.format_destination_name(collection_name)
        self.upload_config.collection = collection_name

        if self.upload_config.auto_schema:
            # Weaviate creates the collection and its properties on the
            # first insert (requires AUTOSCHEMA_ENABLED=true)
            return False
        if self.upload_config.flatten_metadata:
            # User manages the collection under flatten mode; precheck validates existence.
            return False

        with self.connection_config.get_client() as weaviate_client:
            if self._collection_present(weaviate_client):
                logger.debug(
                    f"Collection with name '{collection_name}' already exists, skipping creation"
                )
                return False
            connectors_dir = Path(__file__).parents[1]
            collection_config_file = connectors_dir / "assets" / "weaviate_collection_config.json"
            with collection_config_file.open() as f:
                collection_config = json.load(f)
            collection_config["class"] = collection_name
            logger.info(f"Creating weaviate collection '{collection_name}' with default configs")
            weaviate_client.collections.create_from_dict(config=collection_config)
            # Keep the memoized existence flag correct now that we've created it.
            self._collection_exists = True
            return True

    def check_for_errors(self, client: "WeaviateClient") -> None:
        failed_uploads = client.batch.failed_objects
        if not failed_uploads:
            return
        for failure in failed_uploads:
            logger.error(
                f"Failed to upload object with id {failure.original_uuid}: {failure.message}"
            )
        reasons = "; ".join(sorted({str(failure.message) for failure in failed_uploads}))
        message = f"Failed to upload to weaviate: {reasons}"
        if self.upload_config.auto_schema:
            # The most common cause with auto_schema is the cluster refusing to
            # auto-create the collection/columns because AUTOSCHEMA_ENABLED is off.
            message += (
                " (auto_schema=true requires AUTOSCHEMA_ENABLED=true in Weaviate; if it is "
                "disabled, pre-create the collection and set auto_schema=false)"
            )
        raise WriteError(message)

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
            if self._collection_present(weaviate_client):
                self.delete_by_record_id(client=weaviate_client, file_data=file_data)
            schema_props: Optional[set[str]] = None
            if self.upload_config.flatten_metadata and not self.upload_config.auto_schema:
                schema_props = self.get_schema_property_names(weaviate_client)
            with self.upload_config.get_batch_client(client=weaviate_client) as batch_client:
                for e in data:
                    vector = e.pop("embeddings", None)
                    if schema_props is not None:
                        properties = self.conform_to_schema(e, schema_props)
                    else:
                        properties = e
                    batch_client.add_object(
                        collection=self.upload_config.collection,
                        properties=properties,
                        vector=vector,
                    )
            self.check_for_errors(client=weaviate_client)
