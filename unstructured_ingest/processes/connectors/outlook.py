import hashlib
import time
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Coroutine, Generator, Optional

from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError, ValueError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    LocationShape,
    SourceRegistryEntry,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

MAX_EMAILS_PER_FOLDER = 1_000_000  # Maximum number of emails per folder

if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.outlook.mail.folders.folder import MailFolder
    from office365.outlook.mail.messages.message import Message


CONNECTOR_TYPE = "outlook"


class OutlookAccessConfig(AccessConfig):
    client_credential: Optional[str] = Field(
        default=None, description="Azure AD App client secret", alias="client_cred"
    )
    oauth_token: Optional[str] = Field(
        default=None,
        description=(
            "OAuth 2.0 access token for delegated user authentication. "
            "Tokens typically expire after ~1 hour."
        ),
    )
    refresh_token: Optional[str] = Field(
        default=None,
        description="OAuth 2.0 refresh token for obtaining new access tokens. "
        "Long-lived; used by the platform to refresh expired access tokens "
        "before each job run.",
    )

    def model_post_init(self, __context: Any) -> None:
        # Use truthiness so empty strings (e.g. from unset env vars) are treated
        # consistently with the runtime auth-mode check in _acquire_token below.
        has_client_cred = bool(self.client_credential)
        has_oauth_token = bool(self.oauth_token)

        if not has_client_cred and not has_oauth_token:
            raise ValueError("either client_cred or oauth_token must be set")

        if has_client_cred and has_oauth_token:
            raise ValueError("cannot use both oauth_token and client_cred authentication")


class OutlookConnectionConfig(ConnectionConfig):
    access_config: Secret[OutlookAccessConfig]
    client_id: Optional[str] = Field(
        default=None,
        description=(
            "Azure AD App client ID. Required for app-only authentication;"
            " not required when using oauth_token."
        ),
    )
    tenant: str = Field(
        default="common", description="ID or domain name associated with your Azure AD instance"
    )
    authority_url: str = Field(
        default="https://login.microsoftonline.com",
        description="Authentication token provider for Microsoft apps",
    )

    @model_validator(mode="after")
    def _require_client_id_without_oauth(self) -> "OutlookConnectionConfig":
        # client_id lives on ConnectionConfig (above) and oauth_token on AccessConfig,
        # so this cross-field rule can't live in either model_post_init alone.
        if not self.access_config.get_secret_value().oauth_token and not self.client_id:
            raise ValueError("client_id is required when oauth_token is not set")
        return self

    def _log_oauth_advisory(self) -> None:
        """Emit a one-shot advisory at precheck time when delegated OAuth is in use.

        Lives on ConnectionConfig so Indexer/Downloader prechecks share one source
        of truth instead of each duplicating the message. Called from precheck
        (once per step instance) rather than from _acquire_token (called per Graph
        request) to avoid log spam during normal indexing.
        """
        if self.access_config.get_secret_value().oauth_token:
            logger.warning("Using OAuth token authentication. Tokens expire after ~1 hour.")

    @requires_dependencies(["msal"], extras="outlook")
    def _acquire_token(self):
        """Acquire token via MSAL, or hand through a delegated OAuth token."""
        from msal import ConfidentialClientApplication

        access_config = self.access_config.get_secret_value()

        if access_config.oauth_token:
            # Delegated user authentication: hand the access token through directly.
            # Tokens typically expire after ~1 hour; refresh is not handled here.
            return {"access_token": access_config.oauth_token, "token_type": "Bearer"}

        # NOTE: It'd be nice to use `msal.authority.AuthorityBuilder` here paired with AZURE_PUBLIC
        # constant as default in the future but they do not fit well with `authority_url` right now
        authority_url = f"{self.authority_url.rstrip('/')}/{self.tenant}"
        app = ConfidentialClientApplication(
            authority=authority_url,
            client_id=self.client_id,
            client_credential=access_config.client_credential,
        )
        token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
        return token

    @requires_dependencies(["office365"], extras="outlook")
    @SourceConnectionError.wrap
    def get_client(self) -> "GraphClient":
        from office365.graph_client import GraphClient

        return GraphClient(self._acquire_token)


class OutlookIndexerConfig(IndexerConfig):
    outlook_folders: list[str] = Field(
        description="Folders to download email messages from. Do not specify subfolders. "
        "Use quotes if there are spaces in folder names.",
        json_schema_extra={"x-runtime-eligible": True},
    )
    recursive: bool = Field(
        default=False,
        description="Recursively download files in their respective folders otherwise stop at the"
        " files in provided folder level.",
        json_schema_extra={"x-runtime-eligible": True},
    )
    user_email: str = Field(
        description="Outlook email to download messages from.",
        json_schema_extra={"x-runtime-eligible": True},
    )


@dataclass
class OutlookIndexer(Indexer):
    index_config: OutlookIndexerConfig
    connection_config: OutlookConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        messages = self._list_messages(recursive=self.index_config.recursive)

        for message in messages:
            yield self._message_to_file_data(message)

    def run_async(self, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        raise NotImplementedError

    @SourceConnectionError.wrap
    def precheck(self) -> None:
        self.connection_config._log_oauth_advisory()
        client = self.connection_config.get_client()
        client.users[self.index_config.user_email].get().execute_query()

    def is_async(self) -> bool:
        return False

    def _list_messages(self, recursive: bool) -> list["Message"]:
        mail_folders = self._get_selected_root_folders()
        messages = []

        while mail_folders:
            mail_folder = mail_folders.pop()
            messages += list(mail_folder.messages.get().top(MAX_EMAILS_PER_FOLDER).execute_query())

            if recursive:
                mail_folders += list(mail_folder.child_folders.get().execute_query())

        return messages

    def _get_selected_root_folders(self) -> list["MailFolder"]:
        client_user = self.connection_config.get_client().users[self.index_config.user_email]
        root_mail_folders = client_user.mail_folders.get().execute_query()

        selected_names_normalized = [
            folder_name.lower() for folder_name in self.index_config.outlook_folders
        ]
        selected_root_mail_folders = [
            folder
            for folder in root_mail_folders
            if folder.display_name.lower() in selected_names_normalized
        ]

        if not selected_root_mail_folders:
            logger.error(
                f"Root folders selected in configuration: {self.index_config.outlook_folders}"
                f"not found for user email {self.index_config.user_email}. Aborting."
            )
            raise ValueError("Root folders selected in configuration not found.")

        return selected_root_mail_folders

    def _message_to_file_data(self, message: "Message") -> FileData:
        fullpath = self._generate_fullpath(message)
        source_identifiers = SourceIdentifiers(filename=fullpath.name, fullpath=str(fullpath))
        return FileData(
            identifier=message.id,
            connector_type=CONNECTOR_TYPE,
            source_identifiers=source_identifiers,
            metadata=FileDataSourceMetadata(
                url=message.resource_url,
                version=message.change_key,
                date_modified=str(
                    message.last_modified_datetime.replace(tzinfo=timezone.utc).timestamp()
                ),
                date_created=str(message.created_datetime.replace(tzinfo=timezone.utc).timestamp()),
                date_processed=str(time.time()),
                record_locator={
                    "message_id": message.id,
                    "user_email": self.index_config.user_email,
                },
            ),
            additional_metadata={
                "sent_from": str(message.sent_from),
                "to_recipients": [str(recipient) for recipient in message.to_recipients],
                "bcc_recipients": [str(recipient) for recipient in message.to_recipients],
                "subject": message.subject,
                "conversation_id": message.conversation_id,
                "is_draft": message.is_draft,
                "is_read": message.is_read,
                "has_attachments": message.has_attachments,
                "importance": message.importance,
            },
            display_name=source_identifiers.fullpath,
        )

    def _generate_fullpath(self, message: "Message") -> Path:
        return Path(hashlib.sha256(message.id.encode("utf-8")).hexdigest()[:16] + ".eml")


class OutlookDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class OutlookDownloader(Downloader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: OutlookConnectionConfig
    download_config: OutlookDownloaderConfig = field(default_factory=OutlookDownloaderConfig)

    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        # NOTE: Indexer should provide source identifiers required to generate the download path
        download_path = self.get_download_path(file_data)
        if download_path is None:
            logger.error(
                "Generated download path is None, source_identifiers might be missingfrom FileData."
            )
            raise ValueError("Generated invalid download path.")

        self._download_message(file_data, download_path)
        return self.generate_download_response(file_data, download_path)

    def is_async(self) -> bool:
        return False

    def _download_message(self, file_data: FileData, download_path: Path) -> None:
        # NOTE: Indexer should supply the record locator in metadata
        if (
            file_data.metadata.record_locator is None
            or "user_email" not in file_data.metadata.record_locator
            or "message_id" not in file_data.metadata.record_locator
        ):
            logger.error(
                f"Invalid record locator in metadata: {file_data.metadata.record_locator}."
                "Keys 'user_email' and 'message_id' must be present."
            )
            raise ValueError("Invalid record locator.")

        user_email = file_data.metadata.record_locator["user_email"]
        message_id = file_data.metadata.record_locator["message_id"]

        message = self.connection_config.get_client().users[user_email].messages[message_id]
        download_path.parent.mkdir(exist_ok=True, parents=True)

        with open(download_path, "wb") as file:
            message.download(file).execute_query()


outlook_source_entry = SourceRegistryEntry(
    indexer=OutlookIndexer,
    indexer_config=OutlookIndexerConfig,
    downloader=OutlookDownloader,
    downloader_config=OutlookDownloaderConfig,
    connection_config=OutlookConnectionConfig,
    location_shape=LocationShape.API_FOLDER,
    location_identity=("indexer_config.user_email", "indexer_config.outlook_folders"),
    emits_record_version=True,
    supports_recursion=True,
)
