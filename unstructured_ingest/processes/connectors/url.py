"""A `url` source connector: ingest a literal list of {url, filename}.

It replaces a bespoke indexer + downloader pair with a normal
Indexer/Downloader pair:

  - UrlIndexer      -- yields FileData for each configured {url, filename}
  - UrlDownloader   -- SSRF-safe HTTP(S) fetch to a local path

The SSRF guard that lived in the playground downloader as an operator env var
(`environment != "dev"`) is expressed here as connector config
(`UrlDownloaderConfig.allow_private_ips`), and the TOCTOU in that original guard
is closed (see `_ssrf_safe_download`).

Registered as `url` in the source registry (`url_source_entry` below, wired via
`add_source_entry` in `processes/connectors/__init__.py`).
"""

import ipaddress
import os
import socket
import tempfile
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


_NAT64_PREFIX = ipaddress.ip_network("64:ff9b::/96")
_CGNAT = ipaddress.ip_network("100.64.0.0/10")


def _embedded_ipv4(v6: ipaddress.IPv6Address) -> "ipaddress.IPv4Address | None":
    """The IPv4 an IPv6 transition address embeds (mapped / 6to4 / NAT64 / compat), else None."""
    if v6.ipv4_mapped:
        return v6.ipv4_mapped
    if v6.sixtofour:
        return v6.sixtofour
    if v6 in _NAT64_PREFIX:
        return ipaddress.IPv4Address(int(v6) & 0xFFFFFFFF)
    # IPv4-compatible ::/96 (deprecated), e.g. ::7f00:1 -> 127.0.0.1 (skip :: and ::1)
    if int(v6) >> 32 == 0 and (int(v6) & 0xFFFFFFFF) not in (0, 1):
        return ipaddress.IPv4Address(int(v6) & 0xFFFFFFFF)
    return None


def _is_public_ipv4(v4: ipaddress.IPv4Address) -> bool:
    # is_global covers private/loopback/etc.; the explicit CGNAT reject makes it
    # version-independent (100.64.0.0/10's is_global is only correct on py>=3.11.9/3.12.4).
    return v4.is_global and v4 not in _CGNAT


def _is_public_address(ip: str) -> bool:
    addr = ipaddress.ip_address(ip)
    if isinstance(addr, ipaddress.IPv4Address):
        return _is_public_ipv4(addr)
    # IPv6 must be globally routable AND, if it embeds an IPv4 (NAT64/6to4/mapped/compat),
    # that IPv4 must be public too — otherwise it can reach a private/metadata host.
    if not addr.is_global:
        return False
    embedded = _embedded_ipv4(addr)
    return embedded is None or _is_public_ipv4(embedded)


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
        # Allowlist: rejects private/loopback/link-local/reserved/multicast/CGNAT and
        # IPv6 transition addrs that embed a private/metadata IPv4. Skipped in dev.
        if not allow_private and not _is_public_address(ip):
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


def _ssrf_safe_download(
    url: str, dest: Path, allow_private: bool, timeout: int, max_redirects: int = 5
) -> None:
    """Stream a validated GET to `dest`. Redirects are followed manually so every hop
    is revalidated; the body is streamed to disk rather than buffered whole, so a large
    download does not spike worker memory."""
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
        with (
            httpx.Client(
                transport=_pinned_transport(pinned), timeout=timeout, follow_redirects=False
            ) as client,
            client.stream("GET", current) as resp,
        ):
            if resp.is_redirect:
                location = resp.headers.get("location")
                if not location:
                    raise IngestValueError("Redirect without Location header")
                current = str(httpx.URL(current).join(location))  # revalidated next iteration
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"GET {current} -> {resp.status_code}")
            # Stream to a temp file in the destination dir, then atomically replace,
            # so a mid-stream failure (dropped connection, timeout, short write) never
            # leaves a truncated file at `dest` for a later pipeline pass to pick up.
            fd, tmp_name = tempfile.mkstemp(
                dir=str(dest.parent), prefix=f".{dest.name}.", suffix=".part"
            )
            tmp_path = Path(tmp_name)
            try:
                with os.fdopen(fd, "wb") as f:
                    for chunk in resp.iter_bytes():
                        f.write(chunk)
                os.replace(tmp_path, dest)
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            return
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

        download_path = self.get_download_path(file_data=file_data)
        download_path.parent.mkdir(parents=True, exist_ok=True)
        _ssrf_safe_download(
            url,
            download_path,
            allow_private=self.download_config.allow_private_ips,
            timeout=self.download_config.timeout_seconds,
        )

        return self.generate_download_response(file_data=file_data, download_path=download_path)


url_source_entry = SourceRegistryEntry(
    indexer=UrlIndexer,
    indexer_config=UrlIndexerConfig,
    downloader=UrlDownloader,
    downloader_config=UrlDownloaderConfig,
    connection_config=UrlConnectionConfig,
)
