import http.server
import json
import os
import ssl
import tempfile
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


@pytest.fixture
def embedder_file() -> Path:
    int_test_dir = Path(__file__).parent
    assets_dir = int_test_dir / "assets"
    embedder_file = assets_dir / "DA-1p-with-duplicate-pages.pdf.json"
    assert embedder_file.exists()
    assert embedder_file.is_file()
    return embedder_file


_EMBEDDINGS_CALLS = Counter()


class MockOpenAIEmbeddingsHandler(http.server.SimpleHTTPRequestHandler):
    """
    Minimal OpenAPI Completions mock server
    """

    def do_POST(self):
        global _EMBEDDINGS_CALLS
        _EMBEDDINGS_CALLS["POST"] += 1
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        body = {
            "data": [{"object": "embedding", "embedding": [], "index": 0}],
            "object": "list",
            "model": "text-embedding-ada-002",
            "usage": {"prompt_tokens": 1, "total_tokens": 2},
        }
        self.wfile.write(json.dumps(body).encode("utf-8"))


@pytest.fixture(scope="module")
def mock_embeddings_server() -> Generator[tuple[int, str, Counter], None, None]:
    """
    Runs a dead-simple HTTPS server on a random port, in a thread, with a custom TLS certificate.
    """

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .sign(private_key, hashes.SHA256())
    )

    with (
        tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as cert_file,
        tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as key_file,
    ):
        cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
        key_file.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )
        cert_fpath = cert_file.name
        privkey_fpath = key_file.name

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=cert_fpath, keyfile=privkey_fpath, password="")
    server_address = "127.0.0.1", 0

    httpd = http.server.HTTPServer(server_address, MockOpenAIEmbeddingsHandler)
    httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
    thread = threading.Thread(target=httpd.serve_forever)
    thread.daemon = True
    thread.start()
    yield httpd.server_port, cert_fpath, _EMBEDDINGS_CALLS
    httpd.shutdown()
    thread.join()
    os.unlink(cert_fpath)
    os.unlink(privkey_fpath)
