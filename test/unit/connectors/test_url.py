import http.server
import socketserver
import threading
from pathlib import Path

import httpx
import pytest

from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors import url as url_mod
from unstructured_ingest.processes.connectors.url import (
    CONNECTOR_TYPE,
    FileReference,
    UrlConnectionConfig,
    UrlDownloaderConfig,
    UrlIndexer,
    UrlIndexerConfig,
    _pinned_transport,
    _safe_filename,
    _ssrf_safe_download,
    _validate_and_pin,
    url_source_entry,
)


class _Handler(http.server.SimpleHTTPRequestHandler):
    redirect_to = "/a.txt"

    def do_GET(self):  # noqa: N802
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", self.redirect_to)
            self.end_headers()
            return
        super().do_GET()

    def log_message(self, *a):
        pass


@pytest.fixture
def server(tmp_path):
    served = tmp_path / "served"
    served.mkdir()
    (served / "a.txt").write_text("hello alpha")
    handler = lambda *a, **k: _Handler(*a, directory=str(served), **k)  # noqa: E731
    httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        yield httpd, f"http://127.0.0.1:{httpd.server_address[1]}"
    finally:
        httpd.shutdown()


# --- indexer ---------------------------------------------------------------
def test_indexer_yields_filedata():
    idx = UrlIndexer(
        connection_config=UrlConnectionConfig(),
        index_config=UrlIndexerConfig(
            files=[FileReference(url="https://x/y.pdf", filename="y.pdf")]
        ),
    )
    fds = list(idx.run())
    assert len(fds) == 1
    assert fds[0].connector_type == CONNECTOR_TYPE
    assert fds[0].metadata.url == "https://x/y.pdf"
    assert fds[0].source_identifiers.filename == "y.pdf"


def test_indexer_precheck_rejects_empty():
    idx = UrlIndexer(connection_config=UrlConnectionConfig(), index_config=UrlIndexerConfig())
    with pytest.raises(IngestValueError):
        idx.precheck()


def test_indexer_sanitizes_path_traversal_filename():
    # a filename with path components must reduce to a safe basename (no dir escape)
    idx = UrlIndexer(
        connection_config=UrlConnectionConfig(),
        index_config=UrlIndexerConfig(
            files=[FileReference(url="https://x/y", filename="../../etc/passwd")]
        ),
    )
    fd = next(iter(idx.run()))
    assert fd.source_identifiers.filename == "passwd"
    assert fd.source_identifiers.rel_path == "passwd"


@pytest.mark.parametrize("bad", ["..", "../", "/", ""])
def test_safe_filename_rejects_pure_traversal(bad):
    with pytest.raises(IngestValueError):
        _safe_filename(bad)


def test_indexer_precheck_rejects_duplicate_filenames():
    # two entries collapsing to the same basename would overwrite each other
    idx = UrlIndexer(
        connection_config=UrlConnectionConfig(),
        index_config=UrlIndexerConfig(
            files=[
                FileReference(url="https://x/a", filename="dup.txt"),
                FileReference(url="https://x/b", filename="sub/dup.txt"),
            ]
        ),
    )
    with pytest.raises(IngestValueError, match="Duplicate"):
        idx.precheck()


def test_download_blocks_private_by_default(tmp_path, server):
    # full pipeline with the production default (allow_private_ips=False) must block the
    # loopback test server — exercises the guard through UrlDownloader, not just _validate_and_pin
    _, base = server
    fd = next(
        iter(
            UrlIndexer(
                connection_config=UrlConnectionConfig(),
                index_config=UrlIndexerConfig(
                    files=[FileReference(url=f"{base}/a.txt", filename="a.txt")]
                ),
            ).run()
        )
    )
    dl = url_source_entry.downloader(
        connection_config=UrlConnectionConfig(),
        download_config=UrlDownloaderConfig(download_dir=tmp_path),  # allow_private_ips=False
    )
    with pytest.raises(IngestValueError, match="Refusing non-public"):
        dl.run(file_data=fd)


# --- downloader happy path -------------------------------------------------
def _downloader(tmp_path, allow_private=True):
    return url_source_entry.downloader(
        connection_config=UrlConnectionConfig(),
        download_config=UrlDownloaderConfig(download_dir=tmp_path, allow_private_ips=allow_private),
    )


def test_download_happy_path(tmp_path, server):
    _, base = server
    fd = next(
        iter(
            UrlIndexer(
                connection_config=UrlConnectionConfig(),
                index_config=UrlIndexerConfig(
                    files=[FileReference(url=f"{base}/a.txt", filename="a.txt")]
                ),
            ).run()
        )
    )
    resp = _downloader(tmp_path).run(file_data=fd)
    assert Path(resp["path"]).read_text() == "hello alpha"
    assert resp["file_data"].local_download_path == str(Path(resp["path"]).resolve())


def test_download_follows_redirect(tmp_path, server):
    _, base = server
    fd = next(
        iter(
            UrlIndexer(
                connection_config=UrlConnectionConfig(),
                index_config=UrlIndexerConfig(
                    files=[FileReference(url=f"{base}/redirect", filename="r.txt")]
                ),
            ).run()
        )
    )
    resp = _downloader(tmp_path).run(file_data=fd)
    assert Path(resp["path"]).read_text() == "hello alpha"


# --- SSRF validator --------------------------------------------------------
def test_validate_and_pin_allows_public():
    assert _validate_and_pin("8.8.8.8", allow_private=False) == "8.8.8.8"


@pytest.mark.parametrize(
    "bad", ["127.0.0.1", "10.0.0.1", "192.168.1.1", "169.254.169.254", "0.0.0.0", "100.64.0.1"]
)
def test_validate_and_pin_blocks_nonpublic(bad):
    with pytest.raises(IngestValueError, match="Refusing non-public"):
        _validate_and_pin(bad, allow_private=False)


def test_validate_and_pin_blocks_when_any_record_private(monkeypatch):
    # DNS returns one public + one private -> reject (no multi-record bypass).
    monkeypatch.setattr(
        url_mod.socket,
        "getaddrinfo",
        lambda *a, **k: [(0, 0, 0, "", ("64.99.218.207", 0)), (0, 0, 0, "", ("10.1.2.3", 0))],
    )
    with pytest.raises(IngestValueError, match="Refusing non-public"):
        _validate_and_pin("evil.example.com", allow_private=False)


def test_redirect_target_is_revalidated(tmp_path, server, monkeypatch):
    # The post-redirect host must be validated, not just the first hop.
    _Handler.redirect_to = "http://evil.internal/secret"
    _, base = server
    seen = []

    def fake_pin(host, allow_private):
        seen.append(host)
        if host == "evil.internal":
            raise IngestValueError("Refusing non-public address 10.0.0.1 for host evil.internal")
        return "127.0.0.1"  # first hop -> reach the local test server

    monkeypatch.setattr(url_mod, "_validate_and_pin", fake_pin)
    try:
        with pytest.raises(IngestValueError, match="Refusing non-public"):
            _ssrf_safe_download(
                f"{base}/redirect", tmp_path / "out", allow_private=False, timeout=5
            )
        assert "evil.internal" in seen  # proves the redirect hop was revalidated
    finally:
        _Handler.redirect_to = "/a.txt"


def test_pinned_transport_connects_to_pinned_ip(server):
    # Guards the private httpx seam: the socket must target the pinned IP, not the
    # URL host. Requesting an unresolvable host but pinning to 127.0.0.1 must still
    # reach the local server. If httpcore renames the backend seam, this fails loudly.
    _, base = server
    port = base.rsplit(":", 1)[1]
    with httpx.Client(
        transport=_pinned_transport("127.0.0.1"), follow_redirects=False, timeout=5
    ) as client:
        resp = client.get(f"http://does-not-resolve.invalid:{port}/a.txt")
    assert resp.status_code == 200
    assert resp.text == "hello alpha"
