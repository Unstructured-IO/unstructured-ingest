import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret

from unstructured_ingest.data_types.file_data import FileData
from unstructured_ingest.error import (
    DestinationConnectionError,
    KeyError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    WriteError,
    safe_error_summary,
)
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
    LocationShape,
)
from unstructured_ingest.utils.constants import RECORD_ID_LABEL
from unstructured_ingest.utils.data_prep import flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    import grpc
    from pymilvus import MilvusClient

CONNECTOR_TYPE = "milvus"


def _grpc_status_code(exc: Exception) -> Optional["grpc.StatusCode"]:
    """Extract the ``grpc.StatusCode`` from either exception type pymilvus raises.

    pymilvus surfaces a transport-level gRPC failure two different ways
    (see pymilvus ``decorators.py``):

    * Status codes in ``IGNORE_RETRY_CODES`` (``UNAUTHENTICATED``,
      ``PERMISSION_DENIED``, ``INVALID_ARGUMENT``, ...) are re-raised as the RAW
      ``grpc.RpcError`` via ``raise e from e`` -- they are NEVER wrapped in a
      ``MilvusException``. ``grpc.RpcError`` exposes the code as a ``.code()``
      *method*.
    * Every other code (e.g. ``NOT_FOUND``) is retried and, once the retry
      budget is exhausted, wrapped into ``MilvusException(e.code, ...)``.
      ``MilvusException`` exposes ``.code`` as a property; for a server-side
      business failure that is an int ``ErrorCode`` (not a client error), and in
      the sync retry-storm path pymilvus even stores the raw
      ``grpc.RpcError.code`` *method* there.

    So ``.code`` may be a ``grpc.StatusCode``, a callable returning one, or an
    int. Only a resolved ``grpc.StatusCode`` counts; anything else returns
    ``None`` and falls through to the platform error.
    """
    import grpc

    code = getattr(exc, "code", None)
    if callable(code):
        try:
            code = code()
        except Exception:
            return None
    return code if isinstance(code, grpc.StatusCode) else None


def _classify_milvus_exception(
    exc: Exception, platform_error: UnstructuredIngestError
) -> UnstructuredIngestError:
    """Reclassify customer-caused Milvus failures as client errors.

    When a gRPC call fails at the transport level (bad/expired credentials,
    permission, malformed request, missing resource) the failure is the
    customer's fault, so surface it as a user/auth error (422/401) instead of a
    platform error that burns the Job Completions SLO. Accepts BOTH the raw
    ``grpc.RpcError`` (the codes pymilvus never wraps -- crucially
    ``UNAUTHENTICATED``) and ``MilvusException``. Anything without a resolvable
    client ``grpc.StatusCode`` (server-side business codes, unknown failures)
    stays ``platform_error``.
    """
    import grpc

    code = _grpc_status_code(exc)
    if code is None:
        return platform_error
    message = safe_error_summary(exc)
    if code == grpc.StatusCode.UNAUTHENTICATED:
        return UserAuthError(f"Milvus authentication failed: {message}")
    if code in (
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.NOT_FOUND,
    ):
        return UserError(f"Milvus rejected the request: {message}")
    return platform_error


@contextmanager
def _reclassify_milvus_errors(
    platform_error_factory: Callable[[Exception], UnstructuredIngestError],
) -> Generator[None, None, None]:
    """Run a Milvus client call, reclassifying customer-caused gRPC failures.

    Catches BOTH ``grpc.RpcError`` (the raw, unwrapped codes -- including the
    ``UNAUTHENTICATED`` case that motivated this fix) and ``MilvusException``,
    and routes them through :func:`_classify_milvus_exception`. When the failure
    is not a recognised client code the block re-raises the caller's platform
    error unchanged, so genuine platform faults still count against the SLO.

    Re-raises ``from None`` (not ``from exc``): the raised error's message is
    already redacted via ``safe_error_summary``, and suppressing the exception
    chain keeps the raw provider text (server free-text / debug_error_string)
    from resurfacing through traceback logging, per the connector redaction
    invariant (CHANGELOG 1.6.31 / 1.7.8).
    """
    import grpc
    from pymilvus import MilvusException

    try:
        yield
    except (grpc.RpcError, MilvusException) as exc:
        raise _classify_milvus_exception(exc, platform_error_factory(exc)) from None


class MilvusAccessConfig(AccessConfig):
    password: Optional[str] = Field(default=None, description="Milvus password")
    token: Optional[str] = Field(default=None, description="Milvus access token")


class MilvusConnectionConfig(ConnectionConfig):
    access_config: Secret[MilvusAccessConfig] = Field(
        default=MilvusAccessConfig(), validate_default=True
    )
    uri: Optional[str] = Field(
        default=None, description="Milvus uri", examples=["http://localhost:19530"]
    )
    user: Optional[str] = Field(default=None, description="Milvus user")
    db_name: Optional[str] = Field(default=None, description="Milvus database name")

    def get_connection_kwargs(self) -> dict[str, Any]:
        access_config = self.access_config.get_secret_value()
        access_config_dict = access_config.model_dump()
        connection_config_dict = self.model_dump()
        connection_config_dict.pop("access_config", None)
        connection_config_dict.update(access_config_dict)
        # Drop any that were not set explicitly
        connection_config_dict = {k: v for k, v in connection_config_dict.items() if v is not None}
        return connection_config_dict

    @requires_dependencies(["pymilvus"], extras="milvus")
    @contextmanager
    def get_client(self) -> Generator["MilvusClient", None, None]:
        from pymilvus import MilvusClient

        client = None
        try:
            client = MilvusClient(**self.get_connection_kwargs())
            yield client
        finally:
            if client:
                client.close()


class MilvusUploadStagerConfig(UploadStagerConfig):
    fields_to_include: Optional[list[str]] = None
    """If set - list of fields to include in the output.
    Unspecified fields are removed from the elements.
    This action takes place after metadata flattening.
    Missing fields will cause stager to throw KeyError."""

    flatten_metadata: bool = True
    """If set - flatten "metadata" key and put contents directly into data"""


@dataclass
class MilvusUploadStager(UploadStager):
    upload_stager_config: MilvusUploadStagerConfig = field(
        default_factory=lambda: MilvusUploadStagerConfig()
    )

    @staticmethod
    def parse_date_string(date_string: str) -> float:
        try:
            timestamp = float(date_string)
            return timestamp
        except ValueError:
            pass

        try:
            dt = datetime.fromisoformat(date_string.replace("Z", "+00:00"))
            return dt.timestamp()
        except ValueError:
            pass

        return parser.parse(date_string).timestamp()

    def conform_dict(self, element_dict: dict, file_data: FileData) -> dict:
        working_data = element_dict.copy()

        if self.upload_stager_config.flatten_metadata:
            metadata: dict[str, Any] = working_data.pop("metadata", {})
            flattened_metadata = flatten_dict(
                metadata,
                separator="_",
                flatten_lists=False,
                remove_none=True,
            )
            working_data.update(flattened_metadata)

        # TODO: milvus sdk doesn't seem to support defaults via the schema yet,
        #  remove once that gets updated
        defaults = {"is_continuation": False}
        for default in defaults:
            if default not in working_data:
                working_data[default] = defaults[default]

        if self.upload_stager_config.fields_to_include:
            data_keys = set(working_data.keys())
            for data_key in data_keys:
                if data_key not in self.upload_stager_config.fields_to_include:
                    working_data.pop(data_key)
            for field_include_key in self.upload_stager_config.fields_to_include:
                if field_include_key not in working_data:
                    raise KeyError(f"Field '{field_include_key}' is missing in data!")

        datetime_columns = [
            "data_source_date_created",
            "data_source_date_modified",
            "data_source_date_processed",
            "last_modified",
        ]

        json_dumps_fields = ["languages", "data_source_permissions_data"]

        for datetime_column in datetime_columns:
            if datetime_column in working_data:
                working_data[datetime_column] = self.parse_date_string(
                    working_data[datetime_column]
                )
        for json_dumps_field in json_dumps_fields:
            if json_dumps_field in working_data:
                working_data[json_dumps_field] = json.dumps(working_data[json_dumps_field])
        working_data[RECORD_ID_LABEL] = file_data.identifier
        return working_data

    def should_include(self, element_dict: dict) -> bool:
        # Elements with empty text are skipped by the embedder and arrive without
        # an "embeddings" key. Milvus rejects inserts that omit a required vector
        # field (not nullable, no default), so drop them here.
        return "embeddings" in element_dict


class MilvusUploaderConfig(UploaderConfig):
    db_name: Optional[str] = Field(
        default=None,
        description="Milvus database name",
        json_schema_extra={"x-runtime-eligible": True},
    )
    collection_name: str = Field(
        description="Milvus collections to write to",
        json_schema_extra={"x-runtime-eligible": True},
    )
    record_id_key: str = Field(
        default=RECORD_ID_LABEL,
        description="searchable key to find entries for the same record on previous runs",
    )


@dataclass
class MilvusUploader(Uploader):
    connection_config: MilvusConnectionConfig
    upload_config: MilvusUploaderConfig
    connector_type: str = CONNECTOR_TYPE

    def has_dynamic_fields_enabled(self) -> bool:
        """Check if the target collection has dynamic fields enabled."""
        try:
            with self.get_client() as client:
                collection_info = client.describe_collection(self.upload_config.collection_name)

                # Check if dynamic field is enabled
                # The schema info should contain enable_dynamic_field or enableDynamicField
                schema_info = collection_info.get(
                    "enable_dynamic_field",
                    collection_info.get("enableDynamicField", False),
                )
                return bool(schema_info)
        except Exception as e:
            logger.warning(f"Could not determine if collection has dynamic fields enabled: {e}")
            return False

    def precheck(self):
        # Note: intentionally not decorated with @DestinationConnectionError.wrap.
        # That wrapper re-wraps every escaping exception into a platform
        # DestinationConnectionError, which would clobber the user/auth
        # reclassification below. We reproduce its catch-all fallback inline.
        try:
            with (
                _reclassify_milvus_errors(
                    lambda exc: DestinationConnectionError(
                        f"failed to precheck Milvus: {safe_error_summary(exc)}"
                    )
                ),
                self.get_client() as client,
            ):
                if not client.has_collection(self.upload_config.collection_name):
                    # A missing target collection is the customer's
                    # configuration problem (a bad/absent resource), not a
                    # platform fault, so classify it as a user error.
                    raise UserError(
                        f"Milvus collection '{self.upload_config.collection_name}' does not exist"
                    )
        except UnstructuredIngestError:
            raise
        except Exception as e:
            raise DestinationConnectionError(
                f"failed to precheck Milvus: {safe_error_summary(e)}"
            ) from None

    @contextmanager
    def get_client(self) -> Generator["MilvusClient", None, None]:
        with self.connection_config.get_client() as client:
            if db_name := self.upload_config.db_name:
                client.using_database(db_name=db_name)
            yield client

    def delete_by_record_id(self, file_data: FileData) -> None:
        logger.info(
            f"deleting any content with metadata {RECORD_ID_LABEL}={file_data.identifier} "
            f"from milvus collection {self.upload_config.collection_name}"
        )
        # Enter the reclassify context BEFORE get_client so a customer-caused
        # auth/permission failure from get_client's using_database() call (issued
        # on __enter__ when db_name is set) is reclassified too, matching precheck.
        with (
            _reclassify_milvus_errors(
                lambda exc: WriteError(
                    f"failed to delete records from Milvus: {safe_error_summary(exc)}"
                )
            ),
            self.get_client() as client,
        ):
            delete_filter = f'{self.upload_config.record_id_key} == "{file_data.identifier}"'
            resp = client.delete(
                collection_name=self.upload_config.collection_name, filter=delete_filter
            )
            logger.info(
                "deleted {} records from milvus collection {}".format(
                    resp["delete_count"], self.upload_config.collection_name
                )
            )

    @requires_dependencies(["pymilvus"], extras="milvus")
    def _prepare_data_for_insert(self, data: list[dict]) -> list[dict]:
        """
        Conforms the provided data to the schema of the target Milvus collection.
        - If dynamic fields are enabled, it ensures JSON-stringified fields are decoded.
        - If dynamic fields are disabled, it filters out any fields not present in the schema.
        """

        dynamic_fields_enabled = self.has_dynamic_fields_enabled()

        # If dynamic fields are enabled, 'languages' field needs to be a list
        if dynamic_fields_enabled:
            logger.debug("Dynamic fields enabled, ensuring 'languages' field is a list.")
            prepared_data = []
            for item in data:
                new_item = item.copy()
                if "languages" in new_item and isinstance(new_item["languages"], str):
                    try:
                        new_item["languages"] = json.loads(new_item["languages"])
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(
                            f"Could not JSON decode languages field: {new_item['languages']}. "
                            "Leaving as string.",
                        )
                prepared_data.append(new_item)
            return prepared_data

        # If dynamic fields are not enabled, we need to filter out the metadata fields
        # to avoid insertion errors for fields not defined in the schema
        with (
            _reclassify_milvus_errors(
                lambda exc: WriteError(
                    f"failed to describe Milvus collection: {safe_error_summary(exc)}"
                )
            ),
            self.get_client() as client,
        ):
            collection_info = client.describe_collection(
                self.upload_config.collection_name,
            )
        schema_fields = {
            field["name"]
            for field in collection_info.get("fields", [])
            if not field.get("auto_id", False)
        }
        # Remove metadata fields that are not part of the base schema
        filtered_data = []
        for item in data:
            filtered_item = {key: value for key, value in item.items() if key in schema_fields}
            filtered_data.append(filtered_item)
        return filtered_data

    @requires_dependencies(["pymilvus"], extras="milvus")
    def insert_results(self, data: list[dict]):
        logger.info(
            f"uploading {len(data)} entries to {self.connection_config.db_name} "
            f"db in collection {self.upload_config.collection_name}"
        )

        prepared_data = self._prepare_data_for_insert(data=data)

        with (
            _reclassify_milvus_errors(
                lambda exc: WriteError(
                    f"failed to upload records to Milvus: {safe_error_summary(exc)}"
                )
            ),
            self.get_client() as client,
        ):
            res = client.insert(
                collection_name=self.upload_config.collection_name, data=prepared_data
            )
            if "err_count" in res and isinstance(res["err_count"], int) and res["err_count"] > 0:
                err_count = res["err_count"]
                raise WriteError(f"failed to upload {err_count} docs")

    def run_data(self, data: list[dict], file_data: FileData, **kwargs: Any) -> None:
        self.delete_by_record_id(file_data=file_data)
        self.insert_results(data=data)


milvus_destination_entry = DestinationRegistryEntry(
    connection_config=MilvusConnectionConfig,
    uploader=MilvusUploader,
    uploader_config=MilvusUploaderConfig,
    upload_stager=MilvusUploadStager,
    upload_stager_config=MilvusUploadStagerConfig,
    location_shape=LocationShape.SEARCH_INDEX,
    location_identity=(
        "connector_config.db_name",
        "uploader_config.db_name",
        "uploader_config.collection_name",
    ),
    supports_recursion=False,
)
