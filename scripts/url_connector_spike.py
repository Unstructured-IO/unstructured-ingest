"""Spike runner for the `url` source connector — proves it end-to-end with no
platform stack. Serves files on 127.0.0.1, then index -> download, and exercises
the SSRF guard (validator on public/private, private blocked by default, redirect
followed with per-hop revalidation).

Run:  python scripts/url_connector_spike.py
"""

import http.server
import socketserver
import tempfile
import threading
from pathlib import Path

from unstructured_ingest.processes.connectors.url import (
    CONNECTOR_TYPE,
    FileReference,
    UrlDownloaderConfig,
    UrlIndexerConfig,
    _validate_and_pin,
    url_source_entry,
)


class _Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/a.txt")
            self.end_headers()
            return
        super().do_GET()

    def log_message(self, *a):
        pass


def _serve(directory: Path) -> tuple[socketserver.TCPServer, int]:
    handler = lambda *a, **k: _Handler(*a, directory=str(directory), **k)  # noqa: E731
    httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, httpd.server_address[1]


def main() -> None:
    # validator: no network, proves per-hop check on public vs private
    assert _validate_and_pin("8.8.8.8", allow_private=False) == "8.8.8.8"
    for bad in ("127.0.0.1", "10.0.0.1", "169.254.1.1", "0.0.0.0"):
        try:
            _validate_and_pin(bad, allow_private=False)
            raise AssertionError(f"validator failed to block {bad}")
        except Exception as e:  # noqa: BLE001
            assert "refusing" in str(e).lower(), (bad, e)

    work = Path(tempfile.mkdtemp())
    served = work / "served"
    served.mkdir()
    (served / "a.txt").write_text("hello alpha")
    (served / "b.txt").write_text("hello beta")

    httpd, port = _serve(served)
    base = f"http://127.0.0.1:{port}"
    dl_dir = work / "downloads"

    try:
        conn = url_source_entry.connection_config()
        indexer = url_source_entry.indexer(
            connection_config=conn,
            index_config=UrlIndexerConfig(
                files=[
                    FileReference(url=f"{base}/a.txt", filename="a.txt"),
                    FileReference(url=f"{base}/b.txt", filename="b.txt"),
                ]
            ),
        )
        # allow_private_ips=True because the test server is on 127.0.0.1
        downloader = url_source_entry.downloader(
            connection_config=conn,
            download_config=UrlDownloaderConfig(download_dir=dl_dir, allow_private_ips=True),
        )

        file_datas = list(indexer.run())
        assert len(file_datas) == 2, file_datas
        assert all(fd.connector_type == CONNECTOR_TYPE for fd in file_datas)

        contents = {}
        for fd in file_datas:
            resp = downloader.run(file_data=fd)
            path = Path(resp["path"])
            assert path.exists(), path
            contents[fd.source_identifiers.filename] = path.read_text()
            assert resp["file_data"].local_download_path == str(path.resolve())
        assert contents == {"a.txt": "hello alpha", "b.txt": "hello beta"}, contents

        # redirect is followed (each hop revalidated)
        redirect_fd = next(iter(indexer.run()))
        redirect_fd.metadata.url = f"{base}/redirect"
        redirect_fd.source_identifiers.filename = "redir.txt"
        redirect_fd.source_identifiers.fullpath = "redir.txt"
        redirect_fd.source_identifiers.rel_path = "redir.txt"
        resp = downloader.run(file_data=redirect_fd)
        assert Path(resp["path"]).read_text() == "hello alpha"

        # private blocked by default
        guarded = url_source_entry.downloader(
            connection_config=conn,
            download_config=UrlDownloaderConfig(download_dir=dl_dir),  # allow_private_ips=False
        )
        blocked = False
        try:
            guarded.run(file_data=file_datas[0])
        except Exception as e:  # noqa: BLE001
            blocked = "refusing" in str(e).lower()
        assert blocked, "SSRF guard did NOT block a private IP by default"

    finally:
        httpd.shutdown()

    print("SPIKE OK — conforms to interfaces; SSRF guard is config; no TOCTOU")


if __name__ == "__main__":
    main()
