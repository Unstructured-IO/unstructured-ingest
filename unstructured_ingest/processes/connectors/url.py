"""A `url` source connector: ingest a literal list of {url, filename}.

It replaces a bespoke indexer + downloader pair with a normal
Indexer/Downloader pair:

  - UrlIndexer      -- yields FileData for each configured {url, filename}
  - UrlDownloader   -- SSRF-safe HTTP(S) fetch to a local path

The SSRF guard that lived in the playground downloader as an operator env var
(`environment != "dev"`) is expressed here as connector config
(`UrlDownloaderConfig.allow_private_ips`), and the TOCTOU in that original guard
is closed (see `_ssrf_safe_get`).

NOT YET REGISTERED. `add_source_entry("url", url_source_entry)` in
`processes/connectors/__init__.py` + the utic_types enum + plugin manifests are
follow-ups. Importing this module has no effect on the shipped registry.
"""

import ipaddress
import socket
import urllib.parse
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator

from pydantic import BaseModel, Field, Secret

from unstructured_ingest.data_types.file_data import (
    FileData,
    FileDataSourceMetadata,
    SourceIdentifiers,
)
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    Indexer,
    IndexerConfig,
)
from unstructured_ingest.processes.connector_registry import SourceRegistryEntry
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    import httpx

CONNECTOR_TYPE = "url"


# --- connection (no auth: these are references we minted) ------------------
class UrlAccessConfig(AccessConfig):
    pass


class UrlConnectionConfig(ConnectionConfig):
    access_config: Secret[UrlAccessConfig] = Field(
        default_factory=UrlAccessConfig, validate_default=True
    )


# --- indexer: "here is a literal list of {url, filename}" ------------------
class FileReference(BaseModel):
    url: str
    filename: str


class UrlIndexerConfig(IndexerConfig):
    files: list[FileReference] = Field(
        default_factory=list,
        description="Explicit list of files to ingest, each a {url, filename}.",
    )


def _safe_filename(filename: str) -> str:
    """Reduce a caller-supplied filename to a safe basename — the download path is
    derived from it, so `../` etc. must not escape the download dir."""
    name = Path(filename).name
    if not name or name in (".", ".."):
        raise IngestValueError(f"Invalid filename: {filename!r}")
    return name


@dataclass
class UrlIndexer(Indexer):
    connection_config: UrlConnectionConfig
    index_config: UrlIndexerConfig

    def precheck(self) -> None:
        if not self.index_config.files:
            raise IngestValueError("No files provided")
        # reject path traversal + collisions up front (the download path derives from filename)
        names = [_safe_filename(f.filename) for f in self.index_config.files]
        if len(set(names)) != len(names):
            raise IngestValueError("Duplicate filenames are not allowed")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for ref in self.index_config.files:
            filename = _safe_filename(ref.filename)
            yield FileData(
                identifier=uuid.uuid5(uuid.NAMESPACE_URL, ref.url).hex,
                connector_type=CONNECTOR_TYPE,
                source_identifiers=SourceIdentifiers(
                    filename=filename,
                    fullpath=filename,
                    rel_path=filename,
                ),
                metadata=FileDataSourceMetadata(url=ref.url),
                display_name=filename,
            )


# --- downloader: HTTP fetch with SSRF guard as *config* -------------------
class UrlDownloaderConfig(DownloaderConfig):
    allow_private_ips: bool = Field(
        default=False,
        description="Permit fetching URLs that resolve to private IPs. "
        "Replaces the playground downloader's `environment != 'dev'` env check.",
    )
    timeout_seconds: int = Field(default=120)


# --- SSRF-safe fetch: no TOCTOU --------------------------------------------
# The naive pattern (resolve -> validate -> get) re-resolves DNS at fetch time,
# so a hostile/rebinding resolver can answer public to the validating lookup and
# private to the fetch. We close that by resolving ONCE, validating EVERY
# returned address, then pinning the httpx transport's socket to the validated
# IP -- httpcore still drives TLS SNI + cert verification off the real hostname,
# so pinning cannot be bypassed. Redirects are disabled and followed manually so
# each hop is revalidated.


def _validate_and_pin(host: str, allow_private: bool) -> str:
    """Resolve host, reject if ANY address is non-public, return one pinned IP."""
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as e:
        raise IngestValueError(f"Failed to resolve host: {host}") from e
    ips = sorted({info[4][0] for info in infos})
    if not ips:
        raise IngestValueError(f"No addresses for host: {host}")
    for ip in ips:
        # is_global is an allowlist: rejects private/loopback/link-local/reserved/
        # multicast/unspecified AND non-globally-routable ranges a denylist misses
        # (e.g. CGNAT 100.64.0.0/10). Skipped when allow_private is set.
        if not allow_private and not ipaddress.ip_address(ip).is_global:
            raise IngestValueError(f"Refusing non-public address {ip} for host {host}")
    return ips[0]  # deterministic pin; all addresses already validated


def _pinned_transport(pinned_ip: str) -> "httpx.HTTPTransport":
    """An httpx transport whose TCP connect targets `pinned_ip`, while httpcore
    keeps TLS SNI/cert bound to the request's real hostname."""
    import httpx
    from httpcore._backends.sync import SyncBackend

    class _PinnedBackend(SyncBackend):
        def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):
            return super().connect_tcp(
                pinned_ip,
                port,
                timeout=timeout,
                local_address=local_address,
                socket_options=socket_options,
            )

    transport = httpx.HTTPTransport(retries=0)
    # Private seam (asserted by test_pinned_transport_connects_to_pinned_ip); if a
    # future httpcore renames it, that test fails loudly rather than silently
    # un-pinning the SSRF guard.
    transport._pool._network_backend = _PinnedBackend()
    return transport


def _ssrf_safe_get(url: str, allow_private: bool, timeout: int, max_redirects: int = 5) -> bytes:
    import httpx

    current = url
    for _ in range(max_redirects + 1):
        parts = urllib.parse.urlsplit(current)
        if parts.scheme not in ("http", "https"):
            raise IngestValueError(f"Unsupported scheme: {parts.scheme!r}")
        host = parts.hostname
        if not host:
            raise IngestValueError(f"No host in url: {current}")
        pinned = _validate_and_pin(host, allow_private)
        with httpx.Client(
            transport=_pinned_transport(pinned), timeout=timeout, follow_redirects=False
        ) as client:
            resp = client.get(current)
        if resp.is_redirect:
            location = resp.headers.get("location")
            if not location:
                raise IngestValueError("Redirect without Location header")
            current = str(httpx.URL(current).join(location))  # revalidated next iteration
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"GET {current} -> {resp.status_code}")
        return resp.content
    raise IngestValueError(f"Too many redirects for url: {url}")


@dataclass
class UrlDownloader(Downloader):
    connection_config: UrlConnectionConfig
    download_config: UrlDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return False

    @requires_dependencies(["httpx"], extras="url")
    def run(self, file_data: FileData, **kwargs: Any) -> DownloadResponse:
        url = file_data.metadata.url
        if not url:
            raise IngestValueError(f"No url on file_data: {file_data.identifier}")

        data = _ssrf_safe_get(
            url,
            allow_private=self.download_config.allow_private_ips,
            timeout=self.download_config.timeout_seconds,
        )

        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        with open(download_path, "wb") as f:
            f.write(data)

        return self.generate_download_response(file_data=file_data, download_path=download_path)


url_source_entry = SourceRegistryEntry(
    indexer=UrlIndexer,
    indexer_config=UrlIndexerConfig,
    downloader=UrlDownloader,
    downloader_config=UrlDownloaderConfig,
    connection_config=UrlConnectionConfig,
)
