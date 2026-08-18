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
    safe_error_summary,
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
    from office365.graph_client import GraphClient
    from office365.onedrive.driveitems.driveItem import DriveItem
    from office365.onedrive.sites.site import Site
    from office365.runtime.client_request_exception import ClientRequestException

CONNECTOR_TYPE = "sharepoint"
LEGACY_DEFAULT_PATH = "Shared Documents"

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

# Without this header the Graph channels endpoint reports `shared` channels as
# `unknownFutureValue`, so a shared Teams channel can't be identified by membershipType.
# (The channel is still enumerated/walkable, but its type is opaque without the header.)
_CHANNELS_PREFER_HEADER = "include-unknown-enum-members"

# Microsoft collaborative-artifact formats stored in SharePoint/OneDrive as Fluid
# Framework / binary containers: Loop components (.loop), legacy Fluid preview files
# (.fluid), and Whiteboard (.whiteboard). Their human-readable content lives server-side
# in the Loop/Fluid/Whiteboard service, not in the downloaded file bytes, so they can
# never be partitioned and must never be downloaded. Matched case-insensitively by suffix.
NON_INGESTIBLE_EXTENSIONS: tuple[str, ...] = (".loop", ".fluid", ".whiteboard")


def _is_non_ingestible_artifact(name: Optional[str]) -> bool:
    """True when ``name`` is a Microsoft collaborative-artifact container (Loop / Fluid /
    Whiteboard). These are opaque Fluid containers with no extractable file content, so
    they are excluded at index time — never downloaded, never partitioned. The
    ``isinstance`` guard keeps a non-string (e.g. a test Mock's auto ``.name``) from
    matching."""
    return isinstance(name, str) and name.lower().endswith(NON_INGESTIBLE_EXTENSIONS)


class SharepointAccessConfig(OnedriveAccessConfig):
    pass


class SharepointConnectionConfig(OnedriveConnectionConfig):
    user_pname: Optional[str] = Field(
        default=None,
        description="User principal name or service account, usually your Azure AD email.",
    )
    site: Optional[str] = Field(
        default=None,
        description="Sharepoint site url. Process either base url e.g \
                    https://[tenant].sharepoint.com  or relative sites \
                    https://[tenant].sharepoint.com/sites/<site_name>. \
                    To process all sites within the tenant pass a site url as \
                    https://[tenant]-admin.sharepoint.com.\
                    This requires the app to be registered at a tenant level. \
                    Optional when indexing a Microsoft Teams team instead (set \
                    team_id on the indexer config); provide either a site or a team_id.",
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
    team_id: Optional[str] = Field(
        default=None,
        description="Microsoft Teams team (group) ID whose channel files should be indexed. "
        "When set, the connector indexes Teams channel files instead of a SharePoint site "
        "document library. Provide either a connection-config 'site' or a 'team_id', not both. "
        "Enumerating channels requires the Team.ReadBasic.All and Channel.ReadBasic.All "
        "application scopes in addition to Sites.Read.All.",
    )
    channels: Optional[list[str]] = Field(
        default=None,
        description="Optional filter of Teams channels to index, by channel display name or "
        "channel ID. Only applies in team mode. Leave empty to index every channel in the team.",
    )


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


def _graph_error(
    error_cls: type[UnstructuredIngestError], message: str, resp: Any
) -> UnstructuredIngestError:
    """Build a typed error from a raw Graph ``requests`` response, stamping the real HTTP
    status (and any ``Retry-After``) onto the instance.

    This is the requests-based analogue of what ``_handle_client_request_exception`` does for
    the office365 SDK path. Without it, a hand-built ``UserAuthError`` for a 403 would surface
    as its class-default status 401 and a 5xx as 400 — misleading status-based retry handling
    and API consumers.
    """
    err = error_cls(message)
    status_code = getattr(resp, "status_code", None)
    if status_code is not None:
        err.status_code = status_code
    retry_after = _parse_retry_after(getattr(resp, "headers", None))
    if retry_after is not None:
        err.retry_after = retry_after
    return err


# Bounded retry for the raw Graph REST calls (channel enumeration, filesFolder resolution, the
# precheck scope probe). The office365 SDK path gets retries via the downloader's tenacity
# wrapper; these hand-rolled ``requests`` calls need their own so a single throttle / 5xx /
# transient network blip retries the request before the error propagates and retries the run.
_GRAPH_MAX_ATTEMPTS = 4
_GRAPH_BACKOFF_BASE = 2.0


def _graph_backoff_seconds(attempt: int, retry_after: Optional[float]) -> float:
    """Exponential backoff for a Graph retry, honoring a server ``Retry-After`` when it is
    longer than the exponential value; both capped at ``_MAX_RETRY_AFTER_WAIT``."""
    base = min(_GRAPH_BACKOFF_BASE * (2 ** (attempt - 1)), _MAX_RETRY_AFTER_WAIT)
    if retry_after is not None:
        return min(max(retry_after, base), _MAX_RETRY_AFTER_WAIT)
    return base


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

    def _is_team_mode(self) -> bool:
        """Team mode indexes Teams channel files; site mode indexes a document library."""
        return bool(self.index_config.team_id)

    def _validate_targeting(self) -> None:
        """Exactly one targeting mode must be configured: a SharePoint site or a Teams team."""
        has_team = bool(self.index_config.team_id)
        has_site = bool(self.connection_config.site)
        if has_team and has_site:
            raise UserError(
                "Provide either a SharePoint 'site' or a Teams 'team_id', not both. "
                "Configure one targeting mode per connector instance."
            )
        if not has_team and not has_site:
            raise UserError(
                "A SharePoint 'site' or a Teams 'team_id' is required to index files."
            )

    @requires_dependencies(["office365"], extras="sharepoint")
    def precheck(self) -> None:
        """Validate the SharePoint (site) or Teams (team) connection before indexing."""
        self.connection_config._log_oauth_advisory()
        self._validate_targeting()

        # Validate authentication - this call will raise UserAuthError if invalid
        self.connection_config.get_token()

        if self._is_team_mode():
            self._precheck_team()
        else:
            self._precheck_site()

    def _precheck_site(self) -> None:
        from office365.runtime.client_request_exception import ClientRequestException

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
        except UnstructuredIngestError:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during SharePoint precheck: {e}", exc_info=True)
            raise UserError(f"Failed to validate SharePoint connection: {str(e)}")

    def _precheck_team(self) -> None:
        """Validate the team is reachable and the required scopes are granted.

        Two probes, together giving team mode the same up-front guarantee site mode's
        precheck already provides:
        - listing channels exercises Team.ReadBasic.All / Channel.ReadBasic.All and the team
          id, raising the typed auth/not-found errors from _list_channels_sync;
        - reading the team's default document library exercises the file-read scope
          (Sites.Read.All / Files.Read.All) that filesFolder resolution and downloads depend
          on, so a missing grant fails fast here instead of turning every channel into a
          successful empty crawl at run time.
        """
        token_resp = self.connection_config.get_token()
        access_token = token_resp.get("access_token") if isinstance(token_resp, dict) else None
        if not access_token:
            raise SourceConnectionError("failed to acquire access token for Teams precheck")
        # Raises UserAuthError (missing scope / forbidden), NotFoundError (bad team), etc.
        self._list_channels_sync(access_token)
        self._probe_files_read_scope_sync(access_token)
        logger.info(
            f"Teams connection validated successfully for team: {self.index_config.team_id}"
        )

    def _probe_files_read_scope_sync(self, access_token: str) -> None:
        """Confirm the app can actually read files, not just enumerate channels.

        Reads the team's default document library (the group drive), which is provisioned at
        team creation and — unlike a channel's filesFolder — is NOT subject to on-demand
        provisioning. That makes it a reliable, provisioning-independent probe of the file-read
        scope: a fresh team 404s on every channel filesFolder, so filesFolder can't be used to
        tell "missing scope" from "not provisioned yet", but the group drive can.

        A 401/403 here means the file-read grant is missing (every channel would otherwise
        crawl empty), so we fail fast. This is a strong-but-not-perfect gate: an app narrowed
        by an Application Access Policy could pass here yet still be denied a private/shared
        channel's separate site, so the run-time filesFolder handling stays the backstop and
        any non-auth hiccup here is logged and allowed through rather than blocking a
        connection we've already validated for enumeration.
        """
        team_id = self.index_config.team_id
        resp = self._graph_get(access_token, f"/groups/{team_id}/drive/root")
        if resp.status_code in (401, 403):
            raise _graph_error(
                UserAuthError,
                f"[HTTP {resp.status_code}] The app can enumerate channels but cannot read "
                f"files for team '{team_id}'. Grant the Sites.Read.All (or Files.Read.All) "
                f"application scope so channel files can be indexed and downloaded: "
                f"{_truncate_body(resp.text)}",
                resp,
            )
        if resp.status_code == 429:
            raise _graph_error(
                RateLimitError,
                f"Rate limited validating file-read scope for team '{team_id}'",
                resp,
            )
        if resp.status_code >= 400:
            # Not an auth failure (e.g. an unexpected 404 on the group drive, or a transient
            # 5xx). Don't block a connection already validated for enumeration — the run-time
            # paths surface genuine read failures.
            logger.warning(
                f"could not fully validate file-read scope for team '{team_id}' "
                f"(HTTP {resp.status_code}); proceeding: {_truncate_body(resp.text)}"
            )

    def drive_item_to_file_data_sync(
        self,
        drive_item: DriveItem,
        raw_permissions: Optional[list[dict[str, Any]]] = None,
    ) -> FileData:
        """Extend the base mapping with the item's drive id + item id.

        Teams private/shared channels live on their *own* SharePoint sites, each with a
        distinct drive, so the downloader can't re-resolve them via the single configured
        site. Carrying drive_id + item_id lets the downloader fetch via
        client.drives[drive_id].items[item_id] regardless of which site the file lives on.
        Harmless for ordinary site files (they get their own drive id too) and backward
        compatible — FileData indexed before this still falls back to site+path.
        """
        file_data = super().drive_item_to_file_data_sync(
            drive_item, raw_permissions=raw_permissions
        )
        parent_ref = getattr(drive_item, "parent_reference", None)
        drive_id = getattr(parent_ref, "driveId", None) if parent_ref is not None else None
        if drive_id:
            record_locator = file_data.metadata.record_locator or {}
            record_locator["drive_id"] = drive_id
            record_locator["item_id"] = drive_item.id
            file_data.metadata.record_locator = record_locator
        return file_data

    async def _emit_chunk(
        self, chunk: list["DriveItem"], access_token: str
    ) -> AsyncIterator[FileData]:
        perms_by_id = await asyncio.to_thread(self._fetch_permissions_raw, chunk, access_token)
        for di in chunk:
            # None = fetch unavailable (skip digest); [] = revoked (real digest).
            yield await self.drive_item_to_file_data(
                drive_item=di,
                raw_permissions=perms_by_id.get(di.id),
            )

    async def _emit_drive_items(
        self, drive_items: "list[DriveItem]", access_token: str
    ) -> AsyncIterator[FileData]:
        """Batch drive items into permission-fetch chunks and yield FileData for each.

        Non-ingestible collaborative artifacts (Loop / Fluid / Whiteboard containers) are
        dropped here — the single chokepoint shared by the site and Teams crawls — so they
        are never permission-fetched, downloaded, or partitioned.
        """
        chunk: list[DriveItem] = []
        for drive_item in drive_items:
            if _is_non_ingestible_artifact(getattr(drive_item, "name", None)):
                logger.debug(
                    "skipping non-ingestible collaborative artifact (never downloaded): %s",
                    getattr(drive_item, "name", None),
                )
                continue
            chunk.append(drive_item)
            if len(chunk) >= PERMISSIONS_BATCH_SIZE:
                async for fd in self._emit_chunk(chunk, access_token):
                    yield fd
                chunk = []
        if chunk:
            async for fd in self._emit_chunk(chunk, access_token):
                yield fd

    @requires_dependencies(["office365"], extras="sharepoint")
    async def run_async(self, **kwargs: Any) -> AsyncIterator[FileData]:
        self._validate_targeting()

        token_resp = await asyncio.to_thread(self.connection_config.get_token)
        if "error" in token_resp:
            raise SourceConnectionError(
                f"[{self.connector_type}]: {token_resp['error']} "
                f"({token_resp.get('error_description')})"
            )

        access_token = token_resp["access_token"]
        client = await asyncio.to_thread(self.connection_config.get_client)

        if self._is_team_mode():
            async for fd in self._run_team_async(client, access_token):
                yield fd
        else:
            async for fd in self._run_site_async(client, access_token):
                yield fd

    async def _run_site_async(
        self, client: "GraphClient", access_token: str
    ) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException

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

        try:
            drive_items = target_drive_item.get_files(
                recursive=self.index_config.recursive
            ).execute_query()
        except ClientRequestException as e:
            logger.error(f"Failed to list SharePoint files for site: {self.connection_config.site}")
            _handle_client_request_exception(e, f"SharePoint site {self.connection_config.site}")

        async for fd in self._emit_drive_items(drive_items, access_token):
            yield fd

    async def _run_team_async(
        self, client: "GraphClient", access_token: str
    ) -> AsyncIterator[FileData]:
        from office365.runtime.client_request_exception import ClientRequestException

        team_id = self.index_config.team_id
        channels = await asyncio.to_thread(self._list_channels_sync, access_token)
        channels = self._filter_channels(channels)
        if not channels:
            logger.warning(f"no channels to index for team '{team_id}'")
            return

        indexed = 0
        skipped: list[tuple[str, str]] = []
        for channel in channels:
            channel_id = channel.get("id")
            channel_name = channel.get("displayName") or channel_id
            membership_type = channel.get("membershipType")
            files_folder, skip_reason = await asyncio.to_thread(
                self._get_channel_files_folder_sync,
                access_token,
                channel_id,
                channel_name,
                membership_type,
            )
            if not files_folder:
                # Benign per-channel skip (never aborts the crawl). Record it so the
                # end-of-run summary makes the omission visible — a clean run must not
                # silently hide a channel whose files were never indexed.
                skipped.append((channel_name, skip_reason or "unknown reason"))
                continue

            indexed += 1
            folder = client.drives[files_folder["drive_id"]].items[files_folder["item_id"]]
            try:
                drive_items = await asyncio.to_thread(self._list_channel_files, folder)
            except ClientRequestException as e:
                # Listing files after the folder resolved can still fail for real reasons
                # (auth / throttle / upstream). Map to a typed error and propagate — a
                # standard/private 401/403 or a 429/5xx here is a genuine failure, not a
                # channel to silently drop. (Verified-benign skips happen only at folder
                # resolution: unprovisioned 404s and forbidden shared channels.)
                _handle_client_request_exception(e, f"Teams channel '{channel_name}' files")

            async for fd in self._emit_drive_items(drive_items, access_token):
                yield fd

        self._log_skipped_channels_summary(team_id, indexed, len(channels), skipped)

    @staticmethod
    def _log_skipped_channels_summary(
        team_id: Optional[str], indexed: int, total: int, skipped: list[tuple[str, str]]
    ) -> None:
        """Emit one consolidated WARNING naming every channel skipped during the crawl.

        Individual skips are also logged inline, but a long crawl buries them; a single summary
        (count + names + reason) ensures a run that completed "successfully" can't quietly hide
        a channel whose files were never indexed — the most likely such cases being a
        cross-tenant shared channel (benign) or an app scoped away from a channel's own site by
        an Application Access Policy (a misconfiguration worth investigating), which are
        indistinguishable from the 403 alone.
        """
        if not skipped:
            return
        detail = "; ".join(f"'{name}' ({reason})" for name, reason in skipped)
        logger.warning(
            "Teams crawl for team '%s' indexed %d of %d channel(s); skipped %d with no files "
            "indexed: %s. Files from skipped channels are NOT included in this run — confirm "
            "each is expected (e.g. an empty/unprovisioned or cross-tenant shared channel) and "
            "not an access misconfiguration (e.g. an Application Access Policy scoping the app "
            "away from a channel's site).",
            team_id,
            indexed,
            total,
            len(skipped),
            detail,
        )

    def _list_channel_files(self, folder: "DriveItem") -> "list[DriveItem]":
        return folder.get_files(recursive=self.index_config.recursive).execute_query()

    def _filter_channels(self, channels: list[dict]) -> list[dict]:
        """Restrict to the requested channels (by display name or id); no filter = all."""
        wanted = self.index_config.channels
        if not wanted:
            return channels
        wanted_set = {w.strip().lower() for w in wanted if w and w.strip()}
        selected = [
            c
            for c in channels
            if (c.get("id", "").lower() in wanted_set)
            or (c.get("displayName", "").lower() in wanted_set)
        ]
        matched = {c.get("id", "").lower() for c in selected}
        matched |= {c.get("displayName", "").lower() for c in selected}
        for w in wanted_set:
            if w not in matched:
                logger.warning(
                    f"requested channel '{w}' not found in team '{self.index_config.team_id}'"
                )
        return selected

    @requires_dependencies(["requests"], extras="sharepoint")
    def _graph_get(self, access_token: str, url: str, prefer: Optional[str] = None):
        """GET a Graph REST URL with bounded retry.

        Retries transient failures — throttling (429), upstream 5xx, and network/timeout
        errors — up to ``_GRAPH_MAX_ATTEMPTS``, backing off and honoring ``Retry-After``, so a
        single blip doesn't propagate. After exhaustion a network error raises a retriable
        ``SourceConnectionNetworkError``; a still-throttled/5xx response is returned so the
        caller maps it to a typed error carrying the real status. Non-transient responses
        (2xx, 401, 403, 404, ...) are returned immediately for the caller to interpret.
        """
        import time

        import requests

        headers = {"Authorization": f"Bearer {access_token}"}
        if prefer:
            headers["Prefer"] = prefer
        full_url = url if url.startswith("http") else f"{GRAPH_BASE_URL}{url}"

        resp = None
        for attempt in range(1, _GRAPH_MAX_ATTEMPTS + 1):
            last_attempt = attempt >= _GRAPH_MAX_ATTEMPTS
            try:
                resp = requests.get(full_url, headers=headers, timeout=60)
            except requests.exceptions.RequestException as exc:
                if last_attempt:
                    # Transient network/timeout, retries exhausted — a retriable typed error so
                    # the run retries, rather than a raw requests exception aborting the crawl.
                    raise SourceConnectionNetworkError(
                        f"network error calling Microsoft Graph ({url}) after {attempt} "
                        f"attempt(s): {safe_error_summary(exc)}"
                    ) from exc
                time.sleep(_graph_backoff_seconds(attempt, None))
                continue

            # Retry transient statuses until the last attempt, then hand the (still-throttled /
            # 5xx) response back so the caller maps it to a typed error with the real status.
            if (resp.status_code == 429 or resp.status_code >= 500) and not last_attempt:
                time.sleep(_graph_backoff_seconds(attempt, _parse_retry_after(resp.headers)))
                continue

            break
        return resp

    def _list_channels_sync(self, access_token: str) -> list[dict]:
        """Enumerate a team's channels via Graph (paginated), sending the Prefer header so
        shared channels are correctly typed. Maps auth/not-found/throttle to typed errors."""
        team_id = self.index_config.team_id
        channels: list[dict] = []
        url: Optional[str] = f"/teams/{team_id}/channels?$select=id,displayName,membershipType"
        while url:
            resp = self._graph_get(access_token, url, prefer=_CHANNELS_PREFER_HEADER)
            if resp.status_code in (401, 403):
                raise _graph_error(
                    UserAuthError,
                    f"[HTTP {resp.status_code}] Access forbidden enumerating channels for team "
                    f"'{team_id}'. The app registration needs the Team.ReadBasic.All and "
                    f"Channel.ReadBasic.All application scopes: {_truncate_body(resp.text)}",
                    resp,
                )
            if resp.status_code == 404:
                raise _graph_error(NotFoundError, f"Team not found: '{team_id}'", resp)
            if resp.status_code == 429:
                raise _graph_error(
                    RateLimitError,
                    f"Rate limited enumerating channels for team '{team_id}'",
                    resp,
                )
            if resp.status_code >= 500:
                raise _graph_error(
                    SourceConnectionNetworkError,
                    f"[HTTP {resp.status_code}] Upstream SharePoint error enumerating channels "
                    f"for team '{team_id}': {_truncate_body(resp.text)}",
                    resp,
                )
            if resp.status_code >= 400:
                raise _graph_error(
                    UserError,
                    f"[HTTP {resp.status_code}] Failed to enumerate channels for team "
                    f"'{team_id}': {_truncate_body(resp.text)}",
                    resp,
                )
            body = resp.json()
            channels.extend(body.get("value", []))
            url = body.get("@odata.nextLink")
        logger.info(f"found {len(channels)} channel(s) in team '{team_id}'")
        return channels

    def _get_channel_files_folder_sync(
        self,
        access_token: str,
        channel_id: str,
        channel_name: str,
        membership_type: Optional[str] = None,
    ) -> tuple[Optional[dict], Optional[str]]:
        """Resolve a channel's files folder.

        Returns ``(folder, None)`` with ``folder == {"drive_id", "item_id"}`` when resolved,
        or ``(None, skip_reason)`` when the channel is skipped for a benign, per-channel
        condition: the on-demand-provisioning 404 ("Folder location for this channel is not
        ready yet", which fires until the channel's Files tab is first opened) and a forbidden
        *shared* channel (which can legitimately live in an external tenant we can't reach).
        The reason is surfaced in the caller's end-of-run skipped-channel summary so a
        clean-looking run can't quietly hide a channel whose files were never indexed.

        A 401/403 on a standard/private channel is NOT skipped: it signals a real permission
        gap (e.g. the app is missing Sites.Read.All), which must surface as UserAuthError
        rather than silently yielding an empty crawl. Throttling (429) and upstream 5xx are
        raised as retriable typed errors so the run is retried instead of permanently dropping
        the channel's files.
        """
        team_id = self.index_config.team_id
        resp = self._graph_get(
            access_token, f"/teams/{team_id}/channels/{channel_id}/filesFolder"
        )
        if resp.status_code == 404:
            logger.info(
                f"channel '{channel_name}' files folder is not provisioned yet "
                f"(open the channel's Files tab to provision it); skipping"
            )
            return None, "files folder not provisioned (its Files tab has never been opened)"
        if resp.status_code in (401, 403):
            # Only a shared channel can legitimately be cross-tenant/unreachable. A 403 on a
            # standard or private channel means we lack the SharePoint scope, so fail loudly
            # instead of masking it as a skip that produces zero files.
            if (membership_type or "").lower() == "shared":
                logger.warning(
                    f"access forbidden to files folder for shared channel '{channel_name}' "
                    f"(may be a cross-tenant shared channel); skipping"
                )
                return None, (
                    f"access forbidden (HTTP {resp.status_code}) — possibly a cross-tenant "
                    "shared channel, or the app is scoped away from this channel's site"
                )
            raise _graph_error(
                UserAuthError,
                f"[HTTP {resp.status_code}] Access forbidden resolving the files folder for "
                f"channel '{channel_name}'. The app registration needs Sites.Read.All (and "
                f"Files.Read.All) to read Teams channel files: {_truncate_body(resp.text)}",
                resp,
            )
        if resp.status_code == 429:
            raise _graph_error(
                RateLimitError,
                f"Rate limited resolving files folder for channel '{channel_name}'",
                resp,
            )
        if resp.status_code >= 500:
            raise _graph_error(
                SourceConnectionNetworkError,
                f"[HTTP {resp.status_code}] Upstream SharePoint error resolving files folder "
                f"for channel '{channel_name}'",
                resp,
            )
        if resp.status_code >= 400:
            logger.warning(
                f"failed to resolve files folder for channel '{channel_name}' "
                f"(HTTP {resp.status_code}); skipping: {_truncate_body(resp.text)}"
            )
            return None, f"unexpected HTTP {resp.status_code} resolving files folder"
        body = resp.json()
        drive_id = (body.get("parentReference") or {}).get("driveId")
        item_id = body.get("id")
        if not drive_id or not item_id:
            logger.warning(
                f"channel '{channel_name}' files folder response missing drive/item id; skipping"
            )
            return None, "files folder response missing drive/item id"
        return {"drive_id": drive_id, "item_id": item_id}, None


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

        record_locator = file_data.metadata.record_locator or {}
        drive_id = record_locator.get("drive_id")
        item_id = record_locator.get("item_id")
        has_drive_ref = bool(drive_id and item_id)

        server_relative_path = (
            file_data.source_identifiers.fullpath if file_data.source_identifiers else None
        )
        # Either a drive-id reference (preferred; works across sites incl. Teams private/
        # shared channels) or a server-relative path (legacy site+path resolution) is enough.
        if not has_drive_ref and not server_relative_path:
            raise ValueError(
                f"file data doesn't have enough information to get "
                f"file content: {file_data.model_dump()}"
            )

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
        def _get_item() -> DriveItem:
            # Prefer drive-id resolution: /drives/{driveId}/items/{itemId} addresses a file
            # on any site — including the separate sites that Teams private/shared channels
            # provision — which the single configured-site path can't reach. MS also
            # recommends storing & reusing driveId+itemId rather than site/library URLs.
            if has_drive_ref:
                try:
                    return client.drives[drive_id].items[item_id].get().execute_query()
                except ClientRequestException as e:
                    _handle_client_request_exception(
                        e, f"SharePoint drive item '{item_id}' (drive '{drive_id}')"
                    )
            # Fallback for FileData indexed before drive_id was captured: resolve via the
            # configured site + server-relative path. Split so the failure is attributed to
            # what actually failed (a 404 on the file fetch means the file is missing, not
            # the site). Both paths preserve the real status/body/correlation headers and
            # map to a typed error the retry classifier can act on.
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
            file = _get_item()
        except UnstructuredIngestError:
            raise
        except Exception as e:
            raise SourceConnectionNetworkError(
                f"Error in connecting to upstream data source: {e}"
            ) from e

        if not file:
            raise NotFoundError(f"file not found: {server_relative_path or item_id}")
        return file


sharepoint_source_entry = SourceRegistryEntry(
    connection_config=SharepointConnectionConfig,
    indexer_config=SharepointIndexerConfig,
    indexer=SharepointIndexer,
    downloader_config=SharepointDownloaderConfig,
    downloader=SharepointDownloader,
)
