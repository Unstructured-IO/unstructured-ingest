import io
import os
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from uuid import NAMESPACE_DNS, uuid4, uuid5

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import (
    ProviderError,
    RateLimitError,
    UserAuthError,
    UserError,
    safe_error_summary,
)
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
    Uploader,
    UploaderConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


# Unity Catalog answers "you may not write here" with 403 and "that catalog, schema or
# volume does not exist" with 404, and a token the workspace rejects with 401. Those are
# the only verdicts the destination write probe is entitled to act on. A throttle, a
# Databricks-side 5xx, a socket error or an SDK exception this mapping does not know are
# not answers to the permission question, so they are logged and allowed through: a
# destination that works today must not start failing its connection test.
_WRITE_PROBE_FATAL_STATUS_CODES = frozenset({401, 403, 404})

# Prefix for the zero-byte file the destination precheck writes and then removes.
# Recognisable on sight in the volume if a cleanup ever fails.
_WRITE_PROBE_FILENAME_PREFIX = "unstructured_precheck_"


def _databricks_status_code(e: Exception) -> Optional[int]:
    """The HTTP status behind a Databricks SDK exception, or None if it is not one."""
    from databricks.sdk.errors.base import DatabricksError
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    if not isinstance(e, DatabricksError):
        return None
    reverse_mapping = {v: k for k, v in STATUS_CODE_MAPPING.items()}
    # Walk the MRO, not type(e): the SDK raises ERROR_CODE_MAPPING subclasses in preference
    # to the status class (a 404 RESOURCE_DOES_NOT_EXIST is ResourceDoesNotExist(NotFound)).
    for cls in type(e).__mro__:
        if (status_code := reverse_mapping.get(cls)) is not None:
            return status_code
    return None


class DatabricksPathMixin(BaseModel):
    volume: str = Field(
        description="Name of volume in the Unity Catalog",
        json_schema_extra={"x-runtime-eligible": True},
    )
    catalog: str = Field(
        description="Name of the catalog in the Databricks Unity Catalog service",
        json_schema_extra={"x-runtime-eligible": True},
    )
    volume_path: Optional[str] = Field(
        default=None,
        description="Optional path within the volume to write to",
        json_schema_extra={"x-runtime-eligible": True},
    )
    databricks_schema: str = Field(
        default="default",
        alias="schema",
        description="Schema associated with the volume to write to in the Unity Catalog service",
        json_schema_extra={"x-runtime-eligible": True},
    )

    @property
    def path(self) -> str:
        path = f"/Volumes/{self.catalog}/{self.databricks_schema}/{self.volume}"
        if self.volume_path:
            path = f"{path}/{self.volume_path}"
        return path


class DatabricksVolumesAccessConfig(AccessConfig):
    token: Optional[str] = Field(default=None, description="Databricks Personal Access Token")


class DatabricksVolumesConnectionConfig(ConnectionConfig, ABC):
    access_config: Secret[DatabricksVolumesAccessConfig]
    host: Optional[str] = Field(
        default=None,
        description="The Databricks host URL for either the "
        "Databricks workspace endpoint or the "
        "Databricks accounts endpoint.",
    )

    def wrap_error(self, e: Exception) -> Exception:
        if isinstance(e, ValueError):
            error_message = e.args[0]
            message_split = error_message.split(":")
            if (message_split[0].endswith("auth")) or (
                "Client authentication failed" in error_message
            ):
                return UserAuthError(safe_error_summary(e))
        if status_code := _databricks_status_code(e):
            if status_code in [401, 403]:
                return UserAuthError(safe_error_summary(e))
            if status_code == 429:
                return RateLimitError(safe_error_summary(e))
            if 400 <= status_code < 500:
                return UserError(safe_error_summary(e))
            if 500 <= status_code < 600:
                return ProviderError(safe_error_summary(e))
        logger.error(f"unhandled exception from databricks: {safe_error_summary(e)}")
        return e

    @requires_dependencies(dependencies=["databricks.sdk"], extras="databricks-volumes")
    def get_client(self) -> "WorkspaceClient":
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config

        config = Config(
            host=self.host,
            **self.access_config.get_secret_value().model_dump(),
        ).with_user_agent_extra(
            "PyDatabricksSdk", os.getenv("UNSTRUCTURED_USER_AGENT", "unstructuredio_oss")
        )

        return WorkspaceClient(config=config)


class DatabricksVolumesIndexerConfig(IndexerConfig, DatabricksPathMixin):
    recursive: bool = False


@dataclass
class DatabricksVolumesIndexer(Indexer, ABC):
    index_config: DatabricksVolumesIndexerConfig
    connection_config: DatabricksVolumesConnectionConfig

    def precheck(self) -> None:
        try:
            client = self.connection_config.get_client()
            # Building the client is not a connection test: with a personal access token
            # the SDK's auth provider is a static header and contacts nothing. Both calls
            # below are live requests. me() proves the credentials and the host; the
            # listing proves the catalog, schema and volume resolve and that the Unity
            # Catalog read grants are in place.
            client.current_user.me()
            # dbfs.list is a generator, so take a single entry: that issues one request
            # and stops, rather than paging through a volume that may hold many files.
            # An empty path is valid, hence the default.
            next(
                iter(client.dbfs.list(path=self.index_config.path, recursive=False)),
                None,
            )
        except Exception as e:
            # from None suppresses the implicit __context__ so the raw SDK exception text
            # cannot resurface through full-traceback logging; wrap_error already redacts.
            raise self.connection_config.wrap_error(e=e) from None

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        try:
            for file_info in self.connection_config.get_client().dbfs.list(
                path=self.index_config.path, recursive=self.index_config.recursive
            ):
                if file_info.is_dir:
                    continue
                rel_path = file_info.path.replace(self.index_config.path, "")
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]
                filename = Path(file_info.path).name
                source_identifiers = SourceIdentifiers(
                    filename=filename,
                    rel_path=rel_path,
                    fullpath=file_info.path,
                )
                yield FileData(
                    identifier=str(uuid5(NAMESPACE_DNS, file_info.path)),
                    connector_type=self.connector_type,
                    source_identifiers=source_identifiers,
                    additional_metadata={
                        "catalog": self.index_config.catalog,
                        "path": file_info.path,
                    },
                    metadata=FileDataSourceMetadata(
                        url=file_info.path,
                        # The Databricks SDK reports this in milliseconds.
                        date_modified=(
                            str(file_info.modification_time / 1000)
                            if file_info.modification_time is not None
                            else None
                        ),
                    ),
                    display_name=source_identifiers.fullpath,
                )
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)


class DatabricksVolumesDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class DatabricksVolumesDownloader(Downloader, ABC):
    download_config: DatabricksVolumesDownloaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    def get_download_path(self, file_data: FileData) -> Path:
        return self.download_config.download_dir / Path(file_data.source_identifiers.relative_path)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        volumes_path = file_data.additional_metadata["path"]
        logger.info(f"Writing {file_data.identifier} to {download_path}")
        try:
            with self.connection_config.get_client().dbfs.download(path=volumes_path) as c:
                read_content = c._read_handle.read()
        except Exception as e:
            raise self.connection_config.wrap_error(e=e)
        with open(download_path, "wb") as f:
            f.write(read_content)
        return self.generate_download_response(file_data=file_data, download_path=download_path)


class DatabricksVolumesUploaderConfig(UploaderConfig, DatabricksPathMixin):
    pass


@dataclass
class DatabricksVolumesUploader(Uploader, ABC):
    upload_config: DatabricksVolumesUploaderConfig
    connection_config: DatabricksVolumesConnectionConfig

    def get_output_path(self, file_data: FileData) -> str:
        if file_data.source_identifiers.relative_path:
            return os.path.join(
                self.upload_config.path,
                f"{file_data.source_identifiers.relative_path.lstrip('/')}.json",
            )
        else:
            return os.path.join(
                self.upload_config.path, f"{file_data.source_identifiers.filename}.json"
            )

    def precheck(self) -> None:
        try:
            client = self.connection_config.get_client()
            # me() is a live request that proves the credentials and the host, as the
            # indexer does since PLU-637; the write probe below proves the rest.
            client.current_user.me()
        except Exception as e:
            # from None suppresses the implicit __context__ so the raw SDK exception text
            # cannot resurface through full-traceback logging; wrap_error already redacts.
            raise self.connection_config.wrap_error(e=e) from None
        self._verify_write_access(client=client)

    def _verify_write_access(self, client: "WorkspaceClient") -> None:
        """Prove this connector can write where it is configured to write.

        ``current_user.me()`` proves only that the token authenticates and the principal
        is enabled. It never reads ``upload_config.path``, so a volume that does not
        exist, a path typo, a principal with no WRITE VOLUME grant and a catalog or
        schema the principal cannot USE all produced a green connector, and the run then
        failed at ``files.upload``.

        The probe is the write itself: a zero-byte file at the configured path, through
        the same ``files.upload`` the run uses, so it collects the same verdict.
        """
        path = self.upload_config.path
        probe_path = os.path.join(path, f"{_WRITE_PROBE_FILENAME_PREFIX}{uuid4().hex[:16]}")
        try:
            client.files.upload(file_path=probe_path, contents=io.BytesIO(b""), overwrite=True)
        except Exception as e:
            if _databricks_status_code(e) not in _WRITE_PROBE_FATAL_STATUS_CODES:
                logger.warning(
                    f"skipping write-access precheck for {path}: the probe failed for a "
                    f"reason that is not a permission answer ({safe_error_summary(e)})"
                )
                return
            wrapped = self.connection_config.wrap_error(e=e)
            # wrap_error classifies and redacts but has no idea what was being written,
            # and the path is exactly the fact the old check hid. Re-raise its verdict
            # with the path attached; from None for the same reason as above.
            raise type(wrapped)(
                f"cannot write to Databricks volume path {path}: {wrapped}"
            ) from None
        self._remove_write_probe(client=client, probe_path=probe_path)

    def _remove_write_probe(self, client: "WorkspaceClient", probe_path: str) -> None:
        """Delete the probe file.

        The fsspec uploader leaves its ``_empty`` marker in the destination forever; this
        does not, because a Unity Catalog volume is routinely read back by a table, an
        Auto Loader stream or another ingest job, so a stray file there is not inert.
        WRITE VOLUME already covers the delete, so cleaning up asks for no grant the run
        does not need. If it fails anyway the write question has already been answered,
        so this warns and names the file left behind rather than failing the check.
        """
        try:
            client.files.delete(file_path=probe_path)
        except Exception as e:
            logger.warning(
                f"write-access precheck could not remove its probe file {probe_path}: "
                f"{safe_error_summary(e)}"
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        output_path = self.get_output_path(file_data=file_data)
        with open(path, "rb") as elements_file:
            try:
                # Read file bytes and wrap in BytesIO to create BinaryIO object
                file_bytes = elements_file.read()
                binary_data = io.BytesIO(file_bytes)
                self.connection_config.get_client().files.upload(
                    file_path=output_path,
                    contents=binary_data,
                    overwrite=True,
                )
            except Exception as e:
                raise self.connection_config.wrap_error(e=e)
