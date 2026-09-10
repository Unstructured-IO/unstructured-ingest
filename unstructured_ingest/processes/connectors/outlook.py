import email
import email.policy
import hashlib
import html
import re
import time
from dataclasses import dataclass, field
from datetime import timezone
from email.message import EmailMessage
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Generator, NamedTuple, Optional

from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import SourceConnectionError, ValueError, safe_error_summary
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

# Graph accepts $top between 1 and 1000 for /messages and warns that large
# full-message pages can 504; get_all() still follows @odata.nextLink to
# fetch every message in the folder, this just bounds each individual page.
MESSAGES_PAGE_SIZE = 100

# $select projection for /messages, restricted to exactly what
# _message_to_file_data (and _generate_fullpath) read off a message. Graph's
# default projection includes the full HTML body; get_all() already pulls
# every page into memory eagerly, and Microsoft recommends $select to reduce
# the risk of a 504 on large pages, so this narrows the payload without
# losing anything: the downloader fetches the full MIME message separately
# via $value. Keep this list in sync with _message_to_file_data.
MESSAGE_SELECT_FIELDS = [
    "id",
    "changeKey",
    "lastModifiedDateTime",
    "createdDateTime",
    "from",
    "toRecipients",
    "subject",
    "conversationId",
    "isDraft",
    "isRead",
    "hasAttachments",
    "importance",
]

# Graph omits uniqueBody unless it is named in $select.
UNIQUE_BODY_FIELD = "uniqueBody"

# The only two renderings the email partitioner will read (its
# VALID_CONTENT_SOURCES), in its own order of preference. It can be configured
# to prefer plain, so these are also the only two that can hide quoted history.
BODY_PART_PREFERENCE = ("html", "plain")

# refold_source="none" stops the default policy rewriting long headers this
# rebuild never touches: it split an Authentication-Results header on a real
# message. linesep keeps Graph's CRLF rather than flattening to bare newlines.
_MIME_POLICY = email.policy.default.clone(refold_source="none", linesep="\r\n")

_MARKUP_TAG = re.compile(r"<[^>]+>")

# Graph's Prefer header calls MIME's "plain" rendering "text". Sending the
# untranslated value is ignored (RFC 7240) and answered with Graph's HTML.
_GRAPH_BODY_RENDERING = {"html": "html", "plain": "text"}

# Rewriting a body would break the signature these carry.
_PROTECTED_CONTENT_TYPES = ("multipart/signed", "multipart/encrypted")

# Headers that say what a body part is and where it sits, rather than what it
# holds. A multipart/related start= points at Content-ID, and Outlook mail
# resolves relative references to inline resources through Content-Location
# (RFC 2557).
_KEPT_BODY_HEADERS = frozenset(
    {
        "content-id",
        "content-location",
        "content-base",
        "content-language",
        "content-description",
        "content-disposition",
    }
)


class _MailMessage(EmailMessage):
    """A message whose named inline parts count as attachments.

    The standard library calls a part an attachment only when its disposition
    says so, which lets a named inline text part win body selection over the
    real body. Parsing through this class makes `get_body` and the sweep below
    agree on what an attachment is.
    """

    def is_attachment(self) -> bool:
        return super().is_attachment() or self.get_filename() is not None


def _parse(raw: bytes) -> _MailMessage:
    """Parse a downloaded message, with every part a `_MailMessage`."""
    return email.message_from_bytes(raw, _MailMessage, policy=_MIME_POLICY)


if TYPE_CHECKING:
    from office365.graph_client import GraphClient
    from office365.outlook.mail.folders.folder import MailFolder
    from office365.outlook.mail.messages.message import Message
    from office365.runtime.http.request_options import RequestOptions


CONNECTOR_TYPE = "outlook"


def _prefer_immutable_ids(request: "RequestOptions") -> None:
    """Ask Graph to return immutable ids for mail resources.

    Without this header, Outlook/Exchange can rotate a message's id when the
    message is moved between folders, breaking downstream record identity
    that keys off FileData.identifier.
    """
    request.set_header("Prefer", 'IdType="ImmutableId"')


def _prefer_body_rendering(mime_subtype: str) -> Callable[["RequestOptions"], None]:
    """Ask Graph to render uniqueBody to match a MIME body part, keeping any other Prefer."""
    preference = f'outlook.body-content-type="{_GRAPH_BODY_RENDERING[mime_subtype]}"'

    def hook(request: "RequestOptions") -> None:
        existing = request.headers.get("Prefer")
        request.set_header("Prefer", f"{existing}, {preference}" if existing else preference)

    return hook


def _has_visible_text(content: Optional[str], *, is_markup: bool) -> bool:
    """Whether a body value holds any words a reader would see.

    A body with no words still arrives as a non-empty string of tags, which
    would otherwise replace a full body with nothing. Stripping tags is wrong
    for plain text though, where "<no comment>" is the whole message, so the
    caller says which it has.
    """
    if not content:
        return False
    if not is_markup:
        return bool(content.strip())
    return bool(html.unescape(_MARKUP_TAG.sub(" ", content)).strip())


class UniqueBody(NamedTuple):
    """Graph's answer for uniqueBody. The rendering rides along because Graph may not honour it."""

    rendering: str
    content: str


def _is_protected(message: EmailMessage) -> bool:
    """Whether the message is signed or encrypted, so its bytes must not change."""
    return any(part.get_content_type() in _PROTECTED_CONTENT_TYPES for part in message.walk())


def _is_unnamed_text_part(part: EmailMessage) -> bool:
    """Whether a part is a rendering of the message body rather than a file."""
    return part.get_content_type() in ("text/plain", "text/html") and not part.is_attachment()


def _body_part(message: EmailMessage) -> Optional[EmailMessage]:
    """The part a partitioner will read, or None if the message has none."""
    return message.get_body(preferencelist=BODY_PART_PREFERENCE)


def _primary_body_part(raw: bytes) -> Optional[EmailMessage]:
    """The part a partitioner will read, or None if the message has none."""
    return _body_part(_parse(raw))


def _decoded_text(part: EmailMessage) -> str:
    """The decoded text of a body part, empty when it cannot be decoded."""
    try:
        content = part.get_content()
    except Exception:  # noqa: BLE001 - an undecodable body is simply not text
        return ""
    return content if isinstance(content, str) else ""


def _drop_other_body_renderings(part: EmailMessage, keep: EmailMessage) -> None:
    """Remove every rendering of this message's body except `keep`.

    Descent stops at anything attached, an attached message included: its own
    body parts are indistinguishable from this message's and must survive.
    A container emptied by the sweep is dropped rather than left unreadable.
    """
    if not part.is_multipart():
        return
    children = part.get_payload()
    if not isinstance(children, list):
        return

    kept = []
    for child in children:
        if child.get_content_maintype() == "message" or child.is_attachment():
            kept.append(child)
        elif child.is_multipart():
            _drop_other_body_renderings(child, keep)
            grandchildren = child.get_payload()
            if not isinstance(grandchildren, list) or grandchildren:
                kept.append(child)
        elif child is keep or not _is_unnamed_text_part(child):
            kept.append(child)

    if len(kept) != len(children):
        part.set_payload(kept)


def _replace_body(message: EmailMessage, body: EmailMessage, content: str) -> bytes:
    """Rewrite `body` with `content` and drop the renderings it supersedes.

    set_content clears every Content-* header on the part and adds a
    MIME-Version a sub-part should not carry, so the headers describing this
    part's identity and place in the message are put back afterwards.
    """
    preserved = [
        (name, value) for name, value in body.items() if name.lower() in _KEPT_BODY_HEADERS
    ]
    had_mime_version = "MIME-Version" in body

    body.set_content(content, subtype=body.get_content_subtype(), charset="utf-8")

    if not had_mime_version:
        del body["MIME-Version"]
    for name, value in preserved:
        body[name] = value

    _drop_other_body_renderings(message, body)
    return message.as_bytes(policy=_MIME_POLICY)


def _reduce_message_body(raw: bytes, unique_content: Optional[str]) -> Optional[bytes]:
    """Return `raw` with its body replaced by `unique_content`, or None to decline.

    Only the body part a partitioner reads is rewritten; headers, attachments
    and inline related parts arrive untouched. None means keep the message as
    downloaded: it has no body part, it is signed or encrypted, or the unique
    body carries no text. An empty body legitimately yields an empty
    uniqueBody, so declining is correct rather than an error.
    """
    message = _parse(raw)
    if _is_protected(message):
        return None

    body = _body_part(message)
    if body is None:
        return None

    if not _has_visible_text(unique_content, is_markup=body.get_content_subtype() == "html"):
        return None

    return _replace_body(message, body, unique_content)


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

        client = GraphClient(self._acquire_token)
        # Registered directly on the pending request's event handler rather
        # than via client.before_execute(): on the pinned 2.6.2 that
        # context-level helper defaults to once=True, unregistering after the
        # first request, and on 3.0.0 it additionally no-ops on a fresh client
        # (early-returns when no query has been queued yet) and scopes the hook
        # to the last queued query's id. Registering here rides every request,
        # including get_all() pagination continuations, on both versions.
        client.pending_request().beforeExecute += _prefer_immutable_ids
        return client


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
        # Guards against a cycle in the child-folder graph. get_all() makes each
        # spin of such a cycle far more expensive than the old single-page .get()
        # (full pagination for messages and child folders on every repeat visit),
        # so a folder id is only ever expanded once.
        visited_folder_ids: set[str] = set()
        messages = []

        while mail_folders:
            mail_folder = mail_folders.pop()
            if mail_folder.id in visited_folder_ids:
                continue
            visited_folder_ids.add(mail_folder.id)

            messages += list(
                mail_folder.messages.select(MESSAGE_SELECT_FIELDS)
                .get_all(page_size=MESSAGES_PAGE_SIZE)
                .execute_query()
            )

            if recursive:
                mail_folders += list(mail_folder.child_folders.get_all().execute_query())

        return messages

    def _get_selected_root_folders(self) -> list["MailFolder"]:
        client_user = self.connection_config.get_client().users[self.index_config.user_email]
        root_mail_folders = client_user.mail_folders.get_all().execute_query()

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
                # An empty-string changeKey would otherwise compare equal to a
                # stored empty-string version, defeating the platform's
                # unchanged-record skip; normalize falsy to None.
                version=message.get_property("changeKey") or None,
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
    exclude_quoted_history: bool = Field(
        default=False,
        description="Keep only the text unique to each message, dropping the quoted history of "
        "earlier messages in the same conversation. Headers and attachments are unchanged. Costs "
        "one extra Graph request per downloaded message. Graph decides what counts as unique, "
        "with three consequences worth knowing: a forwarded message keeps only the comment its "
        "sender added and not the text they forwarded; a reply typed inside the quoted block "
        "rather than above it can lose that text; and where a message carries both an HTML and a "
        "plain-text body, the HTML one is kept. Turning this on does not rewrite messages already "
        "downloaded, since their record version has not changed.",
    )


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

        client = self.connection_config.get_client()
        message = client.users[user_email].messages[message_id]
        download_path.parent.mkdir(exist_ok=True, parents=True)

        with open(download_path, "wb") as file:
            message.download(file).execute_query()

        if self.download_config.exclude_quoted_history:
            self._exclude_quoted_history(client, user_email, message_id, download_path)

    def _exclude_quoted_history(
        self, client: "GraphClient", user_email: str, message_id: str, download_path: Path
    ) -> None:
        """Rewrite the downloaded message to hold only its own new text.

        Runs after the download, because the downloaded message decides which
        rendering to ask Graph for. Any message Graph cannot supply a unique
        body for is left exactly as downloaded, so the worst case is the
        behaviour of leaving this setting off.
        """
        message = _parse(download_path.read_bytes())
        body = _body_part(message)
        if body is None:
            logger.info(
                f"Message {message_id} has no body part to reduce, keeping it as downloaded."
            )
            return
        if _is_protected(message):
            logger.info(f"Message {message_id} is signed or encrypted, keeping it as downloaded.")
            return

        mime_subtype = body.get_content_subtype()
        try:
            unique = self._fetch_unique_body(client, user_email, message_id, mime_subtype)
        except Exception as e:
            # The full message is already on disk and usable, so a throttled or
            # failed extra request must not fail the record.
            logger.warning(
                f"Could not read the unique body of message {message_id}, "
                f"keeping the full body including any quoted history: {safe_error_summary(e)}"
            )
            return

        rendering = unique.rendering if unique else ""
        content = unique.content if unique else None
        # Angle brackets in Graph's answer are markup only if the answer is not text.
        if not _has_visible_text(content, is_markup=rendering != "text"):
            if _has_visible_text(_decoded_text(body), is_markup=mime_subtype == "html"):
                logger.warning(
                    f"Graph returned no unique body for message {message_id}, "
                    "keeping the full body including any quoted history."
                )
            else:
                logger.info(f"Message {message_id} has an empty body, keeping it as downloaded.")
            return

        expected = _GRAPH_BODY_RENDERING[mime_subtype]
        if rendering and rendering != expected:
            # Graph may ignore an unsupported Prefer header and answer in its own
            # default, which would put markup into a plain-text part.
            logger.warning(
                f"Graph answered with a {rendering} unique body for message "
                f"{message_id} where {expected} was requested, keeping the full body."
            )
            return

        self._write_reduced(message, body, content, message_id, download_path)

    @staticmethod
    def _write_reduced(
        message: EmailMessage,
        body: EmailMessage,
        content: str,
        message_id: str,
        download_path: Path,
    ) -> None:
        """Stage the reduced message beside the target, then move it into place.

        The download already on disk is complete and usable, so a failure here
        leaves it alone rather than failing the record: one unwritable file must
        not fail the run.
        """
        staged = download_path.with_name(download_path.name + ".reduced")
        try:
            staged.write_bytes(_replace_body(message, body, content))
            staged.replace(download_path)
        except Exception as e:
            logger.warning(
                f"Could not write the reduced body of message {message_id}, "
                f"keeping the full body including any quoted history: {safe_error_summary(e)}"
            )
        finally:
            staged.unlink(missing_ok=True)

    @requires_dependencies(["office365"], extras="outlook")
    def _fetch_unique_body(
        self, client: "GraphClient", user_email: str, message_id: str, mime_subtype: str
    ) -> Optional[UniqueBody]:
        """Read uniqueBody for one message, rendered to match its body part.

        Reuses the client the download already built, so this costs one request
        and no second token acquisition. The value comes off the raw property
        dict because the pinned office365 client declares no typed accessor for
        uniqueBody, the same way the indexer reads changeKey.
        """
        # Deregistered afterwards: the hook appends, so one left on the client
        # would stack a second preference onto every later request.
        prefer = _prefer_body_rendering(mime_subtype)
        client.pending_request().beforeExecute += prefer
        try:
            message = client.users[user_email].messages[message_id]
            message.select(["id", UNIQUE_BODY_FIELD]).get()
            client.execute_query()
        finally:
            client.pending_request().beforeExecute -= prefer

        value = message.get_property(UNIQUE_BODY_FIELD)
        if not isinstance(value, dict):
            return None
        return UniqueBody(
            rendering=value.get("contentType") or "", content=value.get("content") or ""
        )


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
