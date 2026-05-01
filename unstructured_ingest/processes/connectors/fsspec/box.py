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
    "editor": ["read", "update"],
    "viewer uploader": ["read"],
    "previewer uploader": ["read"],
    "viewer": ["read"],
    "previewer": ["read"],
    # "uploader" excluded — write-only, cannot view content
}

if TYPE_CHECKING:
    from boxfs import BoxFileSystem

CONNECTOR_TYPE = "box"


class BoxIndexerConfig(FsspecIndexerConfig):
    pass


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


class BoxDownloaderConfig(FsspecDownloaderConfig):
    max_num_metadata_permissions: int = Field(
        500, description="Approximate maximum number of permissions included in metadata"
    )


@dataclass
class BoxDownloader(FsspecDownloader):
    protocol: str = "box"
    connection_config: BoxConnectionConfig
    connector_type: str = CONNECTOR_TYPE
    download_config: Optional[BoxDownloaderConfig] = field(default_factory=BoxDownloaderConfig)
    _folder_collab_cache: dict = field(default_factory=OrderedDict)
    _folder_collab_cache_max_size: int = 5

    def _get_collaborations_for_folder(self, client, folder_id: str) -> list[dict]:
        if folder_id in self._folder_collab_cache:
            self._folder_collab_cache.move_to_end(folder_id)
            logger.debug(f"Retrieved cached collaborations for folder {folder_id}")
            return self._folder_collab_cache[folder_id]

        collabs = []
        try:
            for collab in client.folder(folder_id).get_collaborations():
                collabs.append(collab.response_object)
        except Exception as e:
            logger.debug(f"Could not retrieve collaborations for folder {folder_id}: {e}")

        if len(self._folder_collab_cache) >= self._folder_collab_cache_max_size:
            self._folder_collab_cache.popitem(last=False)
        self._folder_collab_cache[folder_id] = collabs
        return collabs

    def _normalize_collaborations(self, collabs: list[dict], normalized: dict, total: list) -> None:
        max_perms = self.download_config.max_num_metadata_permissions
        for collab in collabs:
            if total[0] >= max_perms:
                break
            if collab.get("status") != "accepted":
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

    def get_permissions_for_file(self, client, file_id: str) -> list[dict]:
        normalized = {
            "read": {"users": set(), "groups": set()},
            "update": {"users": set(), "groups": set()},
            "delete": {"users": set(), "groups": set()},
        }
        total = [0]  # mutable counter passed into helper

        try:
            file_obj = client.file(file_id).get(fields=["path_collection", "has_collaborations"])
            path_entries = (
                file_obj.response_object.get("path_collection", {}).get("entries", [])
            )
            for folder_entry in path_entries:
                folder_id = folder_entry.get("id")
                # skip root "All Files" folder — it never has meaningful collaborations
                if folder_id and folder_id != "0":
                    folder_collabs = self._get_collaborations_for_folder(client, folder_id)
                    self._normalize_collaborations(folder_collabs, normalized, total)
        except Exception as e:
            logger.debug(f"Could not retrieve path_collection for file {file_id}: {e}")

        try:
            file_collabs = [c.response_object for c in client.file(file_id).get_collaborations()]
            self._normalize_collaborations(file_collabs, normalized, total)
        except Exception as e:
            logger.debug(f"Could not retrieve collaborations for file {file_id}: {e}")

        for role_dict in normalized.values():
            for key in role_dict:
                role_dict[key] = sorted(role_dict[key])

        logger.debug(f"normalized permissions generated for file {file_id}: {normalized}")
        return [{k: v} for k, v in normalized.items()]

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        response = super().run(file_data=file_data, **kwargs)
        file_id = (file_data.metadata.record_locator or {}).get("file_id")
        if file_id:
            try:
                client = self.connection_config.get_box_client()
                file_data.metadata.permissions_data = self.get_permissions_for_file(
                    client, file_id
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
