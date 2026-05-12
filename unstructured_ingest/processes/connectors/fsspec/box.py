from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING, Annotated, Any, Generator, Optional

from dateutil import parser
from pydantic import Field, Secret
from pydantic.functional_validators import BeforeValidator

from unstructured_ingest.data_types.file_data import FileData, FileDataSourceMetadata
from unstructured_ingest.error import ProviderError, UserAuthError, UserError
from unstructured_ingest.interfaces import DownloadResponse
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecDownloader,
    FsspecDownloaderConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)
from unstructured_ingest.processes.connectors.utils import conform_string_to_dict
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

BOX_ROLE_MAPPING: dict[str, list[str]] = {
    "owner": ["read", "update", "delete"],
    "co-owner": ["read", "update", "delete"],
    "editor": ["read", "update", "delete"],
    "viewer uploader": ["read"],
    "viewer": ["read"],
    # "previewer" and "previewer uploader" excluded — Box previewers can render
    # content in the web UI but cannot download, so granting "read" would
    # overstate access to downstream ACL consumers expecting download capability.
    # "uploader" excluded — write-only, cannot view content.
}

if TYPE_CHECKING:
    from boxfs import BoxFileSystem

CONNECTOR_TYPE = "box"


def _normalize_collaborations(
    collabs: list[dict], normalized: dict, total: list, max_perms: int
) -> None:
    for collab in collabs:
        if total[0] >= max_perms:
            break
        if collab.get("status") != "accepted":
            continue
        # Access-only collabs are hidden, scoped grants (e.g. shared-link backing) and do
        # not cascade like normal collabs — including them would overgrant siblings.
        if collab.get("is_access_only"):
            continue
        accessible_by = collab.get("accessible_by") or {}
        entity_type = accessible_by.get("type")
        if entity_type not in ("user", "group"):
            continue
        if entity_type == "group" and accessible_by.get("group_type") == "all_users_group":
            continue
        entity_id = accessible_by.get("id")
        if not entity_id:
            continue
        operations = BOX_ROLE_MAPPING.get(collab.get("role", ""), [])
        if not operations:
            continue
        type_key = entity_type + "s"
        for op in operations:
            normalized[op][type_key].add(entity_id)
        total[0] += 1


def _get_collaborations_for_folder(client, folder_id: str, cache: OrderedDict) -> list[dict]:
    if folder_id in cache:
        cache.move_to_end(folder_id)
        logger.debug(f"Retrieved cached collaborations for folder {folder_id}")
        return cache[folder_id]

    collabs = []
    try:
        for collab in client.folder(folder_id).get_collaborations(
            fields=["accessible_by", "role", "status", "is_access_only"]
        ):
            collabs.append(collab.response_object)
    except Exception as e:
        # Don't cache on failure — a transient 503/timeout would otherwise silently
        # zero out permissions for every descendant file sharing this ancestor for
        # the rest of the indexer run.
        logger.warning(f"Could not retrieve collaborations for folder {folder_id}: {e}")
        return collabs

    if len(cache) >= 5:
        cache.popitem(last=False)
    cache[folder_id] = collabs
    return collabs


def _get_permissions_for_file(
    client, file_id: str, cache: OrderedDict, max_perms: int = 500
) -> list[dict]:
    normalized = {
        "read": {"users": set(), "groups": set()},
        "update": {"users": set(), "groups": set()},
        "delete": {"users": set(), "groups": set()},
    }
    total = [0]

    # Default True so a failed file.get() doesn't suppress the direct-collab fetch.
    has_direct_collabs = True
    try:
        file_obj = client.file(file_id).get(fields=["path_collection", "has_collaborations"])
        response_obj = file_obj.response_object
        path_entries = response_obj.get("path_collection", {}).get("entries", [])
        has_direct_collabs = bool(response_obj.get("has_collaborations", True))
        for folder_entry in path_entries:
            folder_id = folder_entry.get("id")
            if folder_id and folder_id != "0":
                folder_collabs = _get_collaborations_for_folder(client, folder_id, cache)
                _normalize_collaborations(folder_collabs, normalized, total, max_perms)
    except Exception as e:
        logger.debug(f"Could not retrieve path_collection for file {file_id}: {e}")

    if has_direct_collabs:
        try:
            file_collabs = [
                c.response_object
                for c in client.file(file_id).get_collaborations(
                    fields=["accessible_by", "role", "status", "is_access_only"]
                )
            ]
            _normalize_collaborations(file_collabs, normalized, total, max_perms)
        except Exception as e:
            logger.debug(f"Could not retrieve collaborations for file {file_id}: {e}")

    for role_dict in normalized.values():
        for key in role_dict:
            role_dict[key] = sorted(role_dict[key])

    logger.debug(f"normalized permissions generated for file {file_id}: {normalized}")
    return [{k: v} for k, v in normalized.items()]


class BoxIndexerConfig(FsspecIndexerConfig):
    max_num_metadata_permissions: int = Field(
        500, description="Approximate maximum number of permissions included in metadata"
    )


class BoxAccessConfig(FsspecAccessConfig):
    box_app_config: Annotated[dict, BeforeValidator(conform_string_to_dict)] = Field(
        description="Box app credentials as a JSON string."
    )


class BoxConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = field(default_factory=lambda: ["box"], init=False)
    access_config: Secret[BoxAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)

    def get_access_config(self) -> dict[str, Any]:
        from boxsdk import JWTAuth

        ac = self.access_config.get_secret_value()
        settings_dict = ac.box_app_config

        # Create and authenticate the JWTAuth object
        oauth = JWTAuth.from_settings_dictionary(settings_dict)
        oauth.authenticate_instance()

        # if not oauth.access_token:
        #     raise SourceConnectionError("Authentication failed: No access token generated.")

        # Prepare the access configuration with the authenticated oauth
        access_kwargs_with_oauth: dict[str, Any] = {
            "oauth": oauth,
        }
        access_config: dict[str, Any] = ac.model_dump()
        access_config.pop("box_app_config", None)
        access_kwargs_with_oauth.update(access_config)

        return access_kwargs_with_oauth

    def wrap_error(self, e: Exception) -> Exception:
        from boxsdk.exception import BoxAPIException, BoxOAuthException

        if isinstance(e, BoxOAuthException):
            return UserAuthError(e.message)
        if not isinstance(e, BoxAPIException):
            logger.error(f"unhandled exception from box ({type(e)}): {e}", exc_info=True)
            return e
        message = e.message or e
        if error_code_status := e.status:
            if 400 <= error_code_status < 500:
                return UserError(message)
            if error_code_status >= 500:
                return ProviderError(message)

        logger.error(f"unhandled exception from box ({type(e)}): {e}", exc_info=True)
        return e

    @requires_dependencies(["boxsdk"], extras="box")
    def get_box_client(self):
        from boxsdk import Client, JWTAuth

        ac = self.access_config.get_secret_value()
        oauth = JWTAuth.from_settings_dictionary(ac.box_app_config)
        oauth.authenticate_instance()
        return Client(oauth)

    @requires_dependencies(["boxfs"], extras="box")
    @contextmanager
    def get_client(self, protocol: str) -> Generator["BoxFileSystem", None, None]:
        with super().get_client(protocol=protocol) as client:
            yield client


@dataclass
class BoxIndexer(FsspecIndexer):
    connection_config: BoxConnectionConfig
    index_config: BoxIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"]
        date_created = None
        date_modified = None
        if modified_at_str := file_info.get("modified_at"):
            date_modified = str(parser.parse(modified_at_str).timestamp())
        if created_at_str := file_info.get("created_at"):
            date_created = str(parser.parse(created_at_str).timestamp())

        file_size = file_info.get("size") if "size" in file_info else None

        version = file_info.get("id")
        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
            "file_id": file_info.get("id"),
        }
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            version=version,
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        client = None
        cache: OrderedDict = OrderedDict()
        try:
            client = self.connection_config.get_box_client()
        except Exception as e:
            logger.warning(f"Could not initialize Box client for permissions fetching: {e}")

        for file_data in super().run(**kwargs):
            if client:
                file_id = (file_data.metadata.record_locator or {}).get("file_id")
                if file_id:
                    try:
                        file_data.metadata.permissions_data = _get_permissions_for_file(
                            client,
                            file_id,
                            cache,
                            self.index_config.max_num_metadata_permissions,
                        )
                    except Exception as e:
                        logger.warning(f"Could not retrieve permissions for file {file_id}: {e}")
            yield file_data


class BoxDownloaderConfig(FsspecDownloaderConfig):
    pass


@dataclass
class BoxDownloader(FsspecDownloader):
    protocol: str = "box"
    connection_config: BoxConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[BoxDownloaderConfig] = field(default_factory=BoxDownloaderConfig)
    _box_client: Any = field(default=None, init=False, repr=False)
    _collab_cache: OrderedDict = field(default_factory=OrderedDict, init=False, repr=False)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        response = super().run(file_data=file_data, **kwargs)
        # permissions_data is set during indexing in BoxIndexer.run(); this fallback handles
        # standalone downloader usage (e.g., CLI, integration tests without the SND plugin layer).
        # Memoize the JWT-auth'd client and ancestor cache across files so we don't re-auth per file.
        if file_data.metadata.permissions_data is None:
            file_id = (file_data.metadata.record_locator or {}).get("file_id")
            if file_id:
                try:
                    if self._box_client is None:
                        self._box_client = self.connection_config.get_box_client()
                    file_data.metadata.permissions_data = _get_permissions_for_file(
                        self._box_client, file_id, self._collab_cache
                    )
                except Exception as e:
                    logger.warning(f"Could not retrieve permissions for file {file_id}: {e}")
        return response


class BoxUploaderConfig(FsspecUploaderConfig):
    pass


@dataclass
class BoxUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: BoxConnectionConfig
    upload_config: BoxUploaderConfig = field(default=None)


box_source_entry = SourceRegistryEntry(
    indexer=BoxIndexer,
    indexer_config=BoxIndexerConfig,
    downloader=BoxDownloader,
    downloader_config=BoxDownloaderConfig,
    connection_config=BoxConnectionConfig,
)

box_destination_entry = DestinationRegistryEntry(
    uploader=BoxUploader,
    uploader_config=BoxUploaderConfig,
    connection_config=BoxConnectionConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
)
