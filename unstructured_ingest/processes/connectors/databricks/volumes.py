import io
import os
from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Optional
from uuid import NAMESPACE_DNS, uuid5

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


# Unity Catalog answers "you may not do that" with 403 and "that catalog, schema or
# volume does not exist" with 404, and a token the workspace rejects with 401. Those are
# the only verdicts the destination precheck is entitled to act on. A throttle, a
# Databricks-side 5xx, a socket error or an SDK exception this mapping does not know are
# not answers to the access question, so they are logged and allowed through: a
# destination that works today must not start failing its connection test.
_PRECHECK_FATAL_STATUS_CODES = frozenset({401, 403, 404})

# Unity Catalog's name for the privilege the uploader needs on the volume, and the
# blanket grant that subsumes it.
_WRITE_VOLUME_PRIVILEGES = frozenset({"WRITE_VOLUME", "ALL_PRIVILEGES"})


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
            # indexer does since PLU-637. It also names the principal the Unity Catalog
            # grant lookup below asks about.
            principal = client.current_user.me().user_name
        except Exception as e:
            if not self._is_precheck_refusal(e):
                # A throttle, a Databricks-side 5xx or a socket error is not an answer
                # about the credentials, and failing on one would break a destination
                # that works today.
                logger.warning(
                    "skipping the Databricks Volumes destination check: the identity "
                    f"call failed for a reason that is not an access answer "
                    f"({safe_error_summary(e)})"
                )
                return
            # from None suppresses the implicit __context__ so the raw SDK exception text
            # cannot resurface through full-traceback logging; wrap_error already redacts.
            raise self.connection_config.wrap_error(e=e) from None
        self._verify_destination(client=client, principal=principal)

    def _is_precheck_refusal(self, e: Exception) -> bool:
        """Whether this exception is an unambiguous denial the precheck may fail on."""
        status_code = _databricks_status_code(e)
        if status_code is not None:
            return status_code in _PRECHECK_FATAL_STATUS_CODES
        # Not an SDK exception. The one failure outside the SDK that is still a denial is
        # a credential the SDK refuses to build a client from at all, which wrap_error
        # recognises and returns as UserAuthError.
        if isinstance(e, ValueError):
            return isinstance(self.connection_config.wrap_error(e=e), UserAuthError)
        return False

    def _verify_destination(self, client: "WorkspaceClient", principal: Optional[str]) -> None:
        """Check the configured volume through Unity Catalog, writing nothing to it.

        ``current_user.me()`` proves only that the token authenticates and the principal
        is enabled. It never reads ``upload_config.path``, so a volume that does not
        exist, a path typo, a principal with no WRITE VOLUME grant and a catalog or
        schema the principal cannot USE all produced a green connector, and the run then
        failed at ``files.upload`` -- after every file had been partitioned, enriched and
        embedded.

        The check reads rather than writes. A Unity Catalog volume is routinely watched:
        a file-arrival trigger or an Auto Loader stream without a glob filter cannot tell
        a zero-byte probe file from real input during the window before it is removed,
        or ever, if the removal fails.

        Exactly one thing fails the check: Unity Catalog saying the volume is not there.
        Everything else it learns is a warning, because no read here answers the write
        question outright -- see the individual methods for why.
        """
        full_name = (
            f"{self.upload_config.catalog}"
            f".{self.upload_config.databricks_schema}"
            f".{self.upload_config.volume}"
        )
        privileges = self._effective_volume_privileges(
            client=client, full_name=full_name, principal=principal
        )
        self._verify_volume_exists(
            client=client,
            full_name=full_name,
            volume_is_known_to_exist=privileges is not None,
        )
        self._warn_when_write_is_not_granted(
            full_name=full_name, principal=principal, privileges=privileges
        )
        self._warn_when_volume_path_is_missing(client=client)

    def _effective_volume_privileges(
        self, client: "WorkspaceClient", full_name: str, principal: Optional[str]
    ) -> Optional[set[str]]:
        """This principal's effective privileges on the volume, or None if UC will not say.

        None doubles as "the volume's existence is still unknown": the lookup resolves
        the securable, so an answer of any kind is also proof the volume is there.
        """
        if not principal:
            logger.warning(
                f"Databricks did not report a user name for these credentials, so the "
                f"WRITE VOLUME grant on {full_name} is unverified"
            )
            return None
        try:
            permissions = client.grants.get_effective("VOLUME", full_name, principal=principal)
        except Exception as e:
            # A principal that does not own the volume may not be allowed to read its
            # grants at all, so a failure here -- 403 included -- says nothing about
            # whether it can write.
            logger.warning(
                f"could not read the Unity Catalog grants on volume {full_name} for "
                f"{principal}, so WRITE VOLUME is unverified ({safe_error_summary(e)})"
            )
            return None
        privileges: set[str] = set()
        for assignment in permissions.privilege_assignments or []:
            for effective in assignment.privileges or []:
                privilege = effective.privilege
                if privilege is not None:
                    privileges.add(getattr(privilege, "value", str(privilege)))
        return privileges

    def _verify_volume_exists(
        self, client: "WorkspaceClient", full_name: str, volume_is_known_to_exist: bool
    ) -> None:
        """Fail the check when Unity Catalog says the volume is not there.

        This is the 404 the precheck exists to catch, and the only refusal it makes.
        """
        try:
            client.volumes.read(full_name)
        except Exception as e:
            if _databricks_status_code(e) != 404 or volume_is_known_to_exist:
                # A 403 is not a write denial: WRITE VOLUME does not imply READ VOLUME,
                # so a principal that can write here may still be refused the metadata
                # read. And if the grant lookup already resolved the securable, a 404
                # here is about visibility, not a missing volume.
                logger.warning(
                    f"could not read Databricks volume {full_name}, so its existence is "
                    f"unverified ({safe_error_summary(e)})"
                )
                return
            wrapped = self.connection_config.wrap_error(e=e)
            # wrap_error classifies and redacts but has no idea what was being read, and
            # the path is exactly the fact the old check hid. Re-raise its verdict with
            # the path attached; from None for the same reason as above.
            raise type(wrapped)(
                f"Databricks volume {full_name} does not exist or is not visible to "
                f"these credentials (configured path {self.upload_config.path}): {wrapped}"
            ) from None

    def _warn_when_write_is_not_granted(
        self, full_name: str, principal: Optional[str], privileges: Optional[set[str]]
    ) -> None:
        if privileges is None or privileges & _WRITE_VOLUME_PRIVILEGES:
            return
        # Not a refusal. The effective-permissions API is documented to expand privileges
        # inherited from parent securables; it does not say it expands group membership,
        # and a grant held through a group is the normal case, so WRITE VOLUME missing
        # from this answer is not proof the write will fail.
        logger.warning(
            f"Unity Catalog does not report WRITE VOLUME on {full_name} for {principal}. "
            f"If the job fails writing, grant WRITE VOLUME on the volume, and USE CATALOG "
            f"and USE SCHEMA on its parents."
        )

    def _warn_when_volume_path_is_missing(self, client: "WorkspaceClient") -> None:
        if not self.upload_config.volume_path:
            # Without a volume_path the destination is the volume root, which
            # _verify_volume_exists has already covered.
            return
        path = self.upload_config.path
        try:
            client.files.get_directory_metadata(directory_path=path)
        except Exception as e:
            # Not a refusal. The uploader writes path/<relative_path>.json and never
            # creates a directory, so the Files API is already relied on to make the
            # parents at upload time and a directory that is not there yet is not a
            # broken destination. A typo in volume_path is still worth saying out loud.
            logger.warning(
                f"could not read the configured Databricks volume path {path}, so a typo "
                f"in volume_path would not be caught here ({safe_error_summary(e)})"
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
