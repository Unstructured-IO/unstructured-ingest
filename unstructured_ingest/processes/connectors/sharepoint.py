from __future__ import annotations

import asyncio
import builtins
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterator, NoReturn, Optional

from pydantic import Field

from unstructured_ingest.data_types.file_data import (
    FileData,
)
from unstructured_ingest.error import (
    NotFoundError,
    RateLimitError,
    SourceConnectionError,
    SourceConnectionNetworkError,
    UnstructuredIngestError,
    UserAuthError,
    UserError,
    ValueError,
)
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.onedrive import (
    PERMISSIONS_BATCH_SIZE,
    OnedriveAccessConfig,
    OnedriveConnectionConfig,
    OnedriveDownloader,
    OnedriveDownloaderConfig,
    OnedriveIndexer,
    OnedriveIndexerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.sites.site import Site
    from office365.runtime.client_request_exception import ClientRequestException

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"


class SharepointAccessConfig(OnedriveAccessConfig):
    pass


class SharepointConnectionConfig(OnedriveConnectionConfig):
    user_pname: Optional[str] = Field(
        default=None,
        description="User principal name or service account, usually your Azure AD email.",
    )
    site: str = Field(
        description="Sharepoint site url. Process either base url e.g \
                    https://[tenant].sharepoint.com  or relative sites \
                    https://[tenant].sharepoint.com/sites/<site_name>. \
                    To process all sites within the tenant pass a site url as \
                    https://[tenant]-admin.sharepoint.com.\
                    This requires the app to be registered at a tenant level"
    )
    library: Optional[str] = Field(
        default=None,
        description="Sharepoint library name. If not provided, the default \
                    drive will be used.",
    )

    def _get_drive_item(self, client_site: Site) -> DriveItem:
        """Helper method to get the drive item for the specified library or default drive."""
        site_drive_item = None
        if self.library:
            for drive in client_site.drives.get().execute_query():
                if drive.name == self.library:
                    logger.info(f"Found the requested library: {self.library}")
                    site_drive_item = drive.get().execute_query().root
                    break

        # If no specific library was found or requested, use the default drive
        if not site_drive_item:
            if self.library:
                logger.warning(
                    f"Library '{self.library}' not found in site '{self.site}'. "
                    "Using the default drive instead."
                )

            site_drive_item = client_site.drive.get().execute_query().root

        return site_drive_item


class SharepointIndexerConfig(OnedriveIndexerConfig):
    # TODO: We can probably make path non-optional on OnedriveIndexerConfig once tested
    path: str = Field(default="")


# Microsoft/SharePoint correlation headers worth preserving for support/diagnosis.
_MS_CORRELATION_HEADERS = (
    "request-id",
    "client-request-id",
    "x-ms-ags-diagnostic",
    "SPRequestGuid",
    "Retry-After",
    "WWW-Authenticate",
)

# Cap the amount of upstream response body carried on the raised error message. Enough to
# diagnose (SharePoint/Graph error bodies are small JSON) without dumping an unbounded payload.
_MAX_BODY_CHARS = 500


def _truncate_body(body: Optional[str]) -> Optional[str]:
    """Cap an upstream response body to ``_MAX_BODY_CHARS`` for both logs and error
    messages, so a large payload isn't written unbounded (and repeatedly on retries)."""
    if not body:
        return body
    return body if len(body) <= _MAX_BODY_CHARS else f"{body[:_MAX_BODY_CHARS]}…"


# Upper bound on how long we'll honor a throttle's Retry-After, so a pathological or
# hostile header can't stall a download far beyond a real throttle window.
_MAX_RETRY_AFTER_WAIT = 300.0


def _parse_retry_after(headers: Any) -> Optional[float]:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into seconds.

    Returns ``None`` if the header is absent or unparseable. SharePoint/Graph send the
    delta-seconds form on throttles; the HTTP-date form is handled for completeness.
    """
    value = headers.get("Retry-After") if headers else None
    if not value:
        return None
    # NB: this module shadows the builtin ``ValueError`` with a custom error class (imported
    # from unstructured_ingest.error), so catch the builtin explicitly here.
    try:
        return max(0.0, float(value))  # delta-seconds form
    except (builtins.TypeError, builtins.ValueError):
        pass
    try:
        from datetime import datetime, timezone
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(value)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (builtins.TypeError, builtins.ValueError):
        pass
    return None


def _honor_retry_after(base_seconds: float, exc: BaseException) -> float:
    """Backoff for a retry: the server's ``Retry-After`` (stamped on the typed error)
    when it exceeds the exponential backoff, capped at ``_MAX_RETRY_AFTER_WAIT``."""
    retry_after = getattr(exc, "retry_after", None)
    if isinstance(retry_after, (int, float)) and retry_after > base_seconds:
        return min(float(retry_after), _MAX_RETRY_AFTER_WAIT)
    return base_seconds


def _handle_client_request_exception(e: ClientRequestException, context: str) -> NoReturn:
    """Map a SharePoint ``ClientRequestException`` to a typed error from its real HTTP
    status, preserving the status/body/Microsoft correlation headers for diagnosis first.

    Shared by the indexer and downloader so both surface the true condition
    (auth vs not-found vs throttle vs other) instead of one opaque label. Preserving the
    real signal *before* labeling is the fix for the downloader that previously re-raised
    every upstream failure as ``SourceConnectionError("Site not found")``.

    The chosen typed class keeps useful semantics (auth vs throttle vs not-found, and which
    errors retry), but the real HTTP status code and response body are *also* passed through
    on the raised error itself — the ``status_code`` is stamped on the instance (shadowing the
    class default, e.g. so a 403 surfaces as 403 rather than ``UserAuthError``'s default 401)
    and a truncated body is appended to the message — so callers/users see the true condition,
    not just the logs.
    """
    response = getattr(e, "response", None)
    status_code = getattr(response, "status_code", None)
    body = getattr(response, "text", None) if response is not None else None
    headers = (getattr(response, "headers", None) or {}) if response is not None else {}
    retry_after = _parse_retry_after(headers)

    # Preserve the real HTTP signal BEFORE applying any label.
    if response is not None:
        correlation = {k: headers.get(k) for k in _MS_CORRELATION_HEADERS if headers.get(k)}
        logger.error(
            "SharePoint upstream error for %s: status_code=%s body=%r correlation=%s",
            context,
            status_code,
            _truncate_body(body),
            correlation,
        )
    else:
        logger.error("SharePoint upstream error for %s: %s", context, e)

    def _raise(error_cls: type[UnstructuredIngestError], summary: str) -> NoReturn:
        prefix = f"[HTTP {status_code}] " if status_code is not None else ""
        message = f"{prefix}{summary}: {_truncate_body(body)}" if body else f"{prefix}{summary}"
        err = error_cls(message)
        # Shadow the class-level default with the real upstream status so it flows through to
        # whatever surfaces the error (e.g. 403 stays a UserAuthError but reports HTTP 403).
        err.status_code = status_code
        # Carry the throttle backoff so the downloader's retry can honor it (the retry sees
        # this typed error, not the original ClientRequestException with its headers).
        if retry_after is not None:
            err.retry_after = retry_after
        raise err from e

    if status_code == 401:
        _raise(UserAuthError, f"Unauthorized access to {context}. Check client credentials")
    if status_code == 403:
        _raise(
            UserAuthError,
            f"Access forbidden to {context}. Check app permissions (Sites.Read.All required)",
        )
    if status_code == 404:
        _raise(NotFoundError, f"Not found: {context}")
    if status_code == 429:
        _raise(RateLimitError, f"Rate limited by SharePoint for {context}")
    if status_code is not None and status_code >= 500:
        # Upstream/provider outage (5xx) is transient — keep it a connection-class error
        # (as the downloader did before) rather than a non-retriable user fault.
        _raise(SourceConnectionNetworkError, f"Upstream SharePoint error for {context}")

    _raise(UserError, f"Failed to access {context}")


@dataclass
class SharepointIndexer(OnedriveIndexer):
    connection_config: SharepointConnectionConfig
    index_config: SharepointIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def _is_root_path(self, path: str) -> bool:
        """Check if the path represents root access (empty string or legacy default)."""
        return not path or not path.strip() or path == LEGACY_DEFAULT_PATH

    def _get_target_drive_item(self, site_drive_item: DriveItem, path: str) -> DriveItem:
        """Get the drive item to search in based on the path."""
        from office365.runtime.client_request_exception import ClientRequestException

        if self._is_root_path(path):
            return site_drive_item
        try:
            return site_drive_item.get_by_path(path).get().execute_query()
        except ClientRequestException as e:
            # Path resolution hits the same upstream — classify it (404/403/429/...) rather
            # than letting a raw ClientRequestException escape unclassified.
            _handle_client_request_exception(e, f"SharePoint path '{path}'")

    def _validate_folder_path(self, site_drive_item: DriveItem, path: str) -> None:
        """Validate that a specific folder path exists and is accessible."""
        from office365.runtime.client_request_exception import ClientRequestException

        try:
            path_item = site_drive_item.get_by_path(path).get().execute_query()
            if path_item is None or not hasattr(path_item, "is_folder"):
                raise UserError(
                    f"SharePoint path '{path}' not found in site {self.connection_config.site}. "
                    f"Check that the path exists and you have access to it"
                )
            logger.info(f"SharePoint folder path '{path}' validated successfully")
        except ClientRequestException as e:
            logger.error(f"Failed to access SharePoint path '{path}': {e}")
            _handle_client_request_exception(e, f"SharePoint path '{path}'")
        except Exception as e:
            logger.error(f"Unexpected error accessing SharePoint path '{path}': {e}")
            raise UserError(f"Failed to validate SharePoint path '{path}': {str(e)}")

    @requires_dependencies(["office365"], extras="sharepoint")
    def precheck(self) -> None:
        """Validate SharePoint connection before indexing."""
        from office365.runtime.client_request_exception import ClientRequestException

        self.connection_config._log_oauth_advisory()

        # Validate authentication - this call will raise UserAuthError if invalid
        self.connection_config.get_token()

        try:
            client = self.connection_config.get_client()
            client_site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = self.connection_config._get_drive_item(client_site)

            path = self.index_config.path
            if not self._is_root_path(path):
                self._validate_folder_path(site_drive_item, path)

            logger.info(
                f"SharePoint connection validated successfully for site: "
                f"{self.connection_config.site}"
            )

        except ClientRequestException as e:
            logger.error(f"SharePoint precheck failed for site: {self.connection_config.site}")
            _handle_client_request_exception(e, f"SharePoint site {self.connection_config.site}")
        except Exception as e:
            logger.error(f"Unexpected error during SharePoint precheck: {e}", exc_info=True)
            raise UserError(f"Failed to validate SharePoint connection: {str(e)}")

    @requires_dependencies(["office365"], extras="sharepoint")
    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException

        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{self.connector_type}]: {token_resp['error']} "
                f"({token_resp.get('error_description')})"
            )

        access_token = token_resp["access_token"]
        client = await asyncio.to_thread(self.connection_config.get_client)
        try:
            client_site = client.sites.get_by_url(self.connection_config.site).get().execute_query()
            site_drive_item = self.connection_config._get_drive_item(client_site)
        except ClientRequestException as e:
            logger.error(f"Failed to access SharePoint site: {self.connection_config.site}")
            _handle_client_request_exception(e, f"SharePoint site {self.connection_config.site}")

        path = self.index_config.path
        target_drive_item = await asyncio.to_thread(
            self._get_target_drive_item, site_drive_item, path
        )

        async def _flush(chunk: list[DriveItem]) -> AsyncIterator[FileData]:
            perms_by_id = await asyncio.to_thread(self._fetch_permissions_raw, chunk, access_token)
            for di in chunk:
                yield await self.drive_item_to_file_data(
                    drive_item=di,
                    raw_permissions=perms_by_id.get(di.id, []),
                )

        try:
            drive_items = target_drive_item.get_files(
                recursive=self.index_config.recursive
            ).execute_query()
        except ClientRequestException as e:
            logger.error(f"Failed to list SharePoint files for site: {self.connection_config.site}")
            _handle_client_request_exception(e, f"SharePoint site {self.connection_config.site}")

        chunk: list[DriveItem] = []
        for drive_item in drive_items:
            chunk.append(drive_item)
            if len(chunk) >= PERMISSIONS_BATCH_SIZE:
                async for fd in _flush(chunk):
                    yield fd
                chunk = []
        if chunk:
            async for fd in _flush(chunk):
                yield fd


class SharepointDownloaderConfig(OnedriveDownloaderConfig):
    max_retries: int = 10


@dataclass
class SharepointDownloader(OnedriveDownloader):
    connection_config: SharepointConnectionConfig
    download_config: SharepointDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    @staticmethod
    def retry_on_status_code(exc):
        # Retry genuine throttles (429) and transient upstream outages (5xx, esp. 503),
        # matching OneDrive's throttle-retry set. Prefer the real HTTP status the shared
        # mapper now stamps on the typed error (every exception raised inside the
        # retry-wrapped `_get_item_by_path` carries it), then fall back to type/string
        # checks for exceptions that didn't come through the mapper.
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int) and (status_code == 429 or status_code >= 500):
            return True
        if isinstance(exc, (RateLimitError, SourceConnectionNetworkError)):
            return True
        error_msg = str(exc).lower()
        return "429" in error_msg or "activitylimitreached" in error_msg or "throttled" in error_msg

    @requires_dependencies(["office365"], extras="sharepoint")
    def _fetch_file(self, file_data: FileData) -> DriveItem:
        from office365.runtime.client_request_exception import ClientRequestException
        from tenacity import (
            before_log,
            retry,
            retry_if_exception,
            stop_after_attempt,
            wait_exponential,
        )

        if file_data.source_identifiers is None or not file_data.source_identifiers.fullpath:
            raise ValueError(
                f"file data doesn't have enough information to get "
                f"file content: {file_data.model_dump()}"
            )

        server_relative_path = file_data.source_identifiers.fullpath
        client = self.connection_config.get_client()

        _exp_wait = wait_exponential(exp_base=2, multiplier=1, min=2, max=10)

        def _wait(retry_state) -> float:
            # Honor a throttle's Retry-After (stamped on the typed error by the mapper)
            # when it exceeds the exponential backoff, so we don't retry before the
            # server's requested window; fall back to exponential otherwise.
            base = _exp_wait(retry_state)
            exc = retry_state.outcome.exception() if retry_state.outcome else None
            return _honor_retry_after(base, exc) if exc is not None else base

        @retry(
            stop=stop_after_attempt(self.download_config.max_retries),
            wait=_wait,
            retry=retry_if_exception(self.retry_on_status_code),
            before=before_log(logger, logging.DEBUG),
            reraise=True,
        )
        def _get_item_by_path() -> DriveItem:
            # Split so the failure is attributed to what actually failed: a 404 on the
            # file fetch means the file is missing, not the site. Both paths preserve the
            # real status/body/correlation headers and map to a typed error (401/403 ->
            # auth, 404 -> not found, 429 -> rate limit, which the retry classifier above
            # then retries) instead of masking every upstream failure as "Site not found".
            try:
                client_site = (
                    client.sites.get_by_url(self.connection_config.site).get().execute_query()
                )
                site_drive_item = self.connection_config._get_drive_item(client_site)
            except ClientRequestException as e:
                _handle_client_request_exception(
                    e, f"SharePoint site {self.connection_config.site}"
                )
            try:
                return site_drive_item.get_by_path(server_relative_path).get().execute_query()
            except ClientRequestException as e:
                _handle_client_request_exception(e, f"SharePoint file '{server_relative_path}'")

        # Intentionally NOT decorated with @SourceConnectionNetworkError.wrap: that coerces
        # sibling typed errors (UserAuthError/RateLimitError/NotFoundError) back to a 400,
        # re-masking the real status. We replicate its catch-all here so genuinely
        # unrecognized failures still surface as a connection error, while typed errors pass
        # through with their true status.
        try:
            file = _get_item_by_path()
        except UnstructuredIngestError:
            raise
        except Exception as e:
            raise SourceConnectionNetworkError(
                f"Error in connecting to upstream data source: {e}"
            ) from e

        if not file:
            raise NotFoundError(f"file not found: {server_relative_path}")
        return file


sharepoint_source_entry = SourceRegistryEntry(
    connection_config=SharepointConnectionConfig,
    indexer_config=SharepointIndexerConfig,
    indexer=SharepointIndexer,
    downloader_config=SharepointDownloaderConfig,
    downloader=SharepointDownloader,
)
