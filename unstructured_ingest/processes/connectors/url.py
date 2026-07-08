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

import http.client
import ipaddress
import socket
import ssl
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import Any, Generator

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


@dataclass
class UrlIndexer(Indexer):
    connection_config: UrlConnectionConfig
    index_config: UrlIndexerConfig

    def precheck(self) -> None:
        if not self.index_config.files:
            raise IngestValueError("No files provided")

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        for ref in self.index_config.files:
            filename = ref.filename
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
# The naive pattern (resolve -> validate -> urlopen) re-resolves DNS inside
# urlopen, so a hostile server can return a public IP to the validating lookup
# and a private one to the fetch. We close that by resolving ONCE, validating
# EVERY returned address, then connecting the socket to the pinned IP (keeping
# the original hostname for Host header + TLS SNI/cert). Redirects are followed
# manually so each hop is revalidated instead of trusting urllib's re-resolve.


class _PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, pinned_ip: str, **kw: Any):
        super().__init__(host, **kw)
        self._pinned_ip = pinned_ip

    def connect(self) -> None:
        self.sock = socket.create_connection((self._pinned_ip, self.port), self.timeout)


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, pinned_ip: str, **kw: Any):
        super().__init__(host, **kw)
        self._pinned_ip = pinned_ip

    def connect(self) -> None:
        sock = socket.create_connection((self._pinned_ip, self.port), self.timeout)
        # server_hostname=self.host -> SNI + cert validated against the real
        # hostname, while the socket is pinned to the validated IP.
        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)


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
        addr = ipaddress.ip_address(ip)
        if not allow_private and (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_reserved
            or addr.is_multicast
            or addr.is_unspecified
        ):
            raise IngestValueError(f"Refusing non-public address {ip} for host {host}")
    return ips[0]  # deterministic pin; all addresses already validated


def _ssrf_safe_get(url: str, allow_private: bool, timeout: int, max_redirects: int = 5) -> bytes:
    for _ in range(max_redirects + 1):
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https"):
            raise IngestValueError(f"Unsupported scheme: {parts.scheme!r}")
        host = parts.hostname
        if not host:
            raise IngestValueError(f"No host in url: {url}")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        pinned = _validate_and_pin(host, allow_private)
        path = parts.path or "/"
        if parts.query:
            path = f"{path}?{parts.query}"

        if parts.scheme == "https":
            conn: http.client.HTTPConnection = _PinnedHTTPSConnection(
                host, pinned, port=port, timeout=timeout, context=ssl.create_default_context()
            )
        else:
            conn = _PinnedHTTPConnection(host, pinned, port=port, timeout=timeout)
        try:
            conn.request("GET", path, headers={"Host": host})
            resp = conn.getresponse()
            if resp.status in (301, 302, 303, 307, 308):
                location = resp.getheader("Location")
                resp.read()
                if not location:
                    raise IngestValueError("Redirect without Location header")
                url = urllib.parse.urljoin(url, location)  # revalidated next iteration
                continue
            if resp.status != 200:
                raise RuntimeError(f"GET {url} -> {resp.status}")
            return resp.read()
        finally:
            conn.close()
    raise IngestValueError(f"Too many redirects for url: {url}")


@dataclass
class UrlDownloader(Downloader):
    connection_config: UrlConnectionConfig
    download_config: UrlDownloaderConfig
    connector_type: str = CONNECTOR_TYPE

    def is_async(self) -> bool:
        return False

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
