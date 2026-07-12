"""Unit tests for SFTP connector.

Tests that SftpUploader creates parent directories before uploading.
SFTP uses a real filesystem where parent directories must exist before a file
can be written. fsspec's SFTPFileSystem.put() bypasses the base class's mkdirs
call (going straight to paramiko), so SftpUploader.run() must call makedirs
explicitly.
"""

from unittest import mock

import pytest

from unstructured_ingest.processes.connectors.fsspec.sftp import (
    SftpAccessConfig,
    SftpConnectionConfig,
    SftpUploader,
    _build_host_key,
    parse_host_public_key,
)


@pytest.fixture
def mock_sftp_client():
    """Create a mock SFTPFileSystem client."""
    client = mock.MagicMock()
    client.makedirs = mock.MagicMock()
    client.upload = mock.MagicMock()
    return client


@pytest.fixture
def sftp_uploader(mock_sftp_client):
    """Create an SftpUploader with mocked dependencies, bypassing __init__."""
    uploader = object.__new__(SftpUploader)
    uploader.connector_type = "sftp"

    # Mock connection config — get_client yields the mock SFTP client
    uploader.connection_config = mock.MagicMock()
    uploader.connection_config.get_client.return_value.__enter__ = mock.MagicMock(
        return_value=mock_sftp_client
    )
    uploader.connection_config.get_client.return_value.__exit__ = mock.MagicMock(
        return_value=False
    )

    # Mock upload config
    uploader.upload_config = mock.MagicMock()
    uploader.upload_config.protocol = "sftp"
    uploader.upload_config.path_without_protocol = "data/output"

    # Silence logging methods inherited from ConnectorLoggingMixin
    uploader.log_upload_start = mock.MagicMock()
    uploader.log_upload_complete = mock.MagicMock()
    uploader.log_error = mock.MagicMock()

    return uploader


def _make_file_data(relative_path: str):
    """Create a mock FileData with the given relative_path."""
    file_data = mock.MagicMock()
    file_data.source_identifiers.relative_path = relative_path
    return file_data


class TestSftpUploaderMakeDirs:
    """Verify that SftpUploader.run() creates parent directories before uploading."""

    def test_makedirs_called_with_parent_of_upload_path(
        self, sftp_uploader, mock_sftp_client, tmp_path
    ):
        """makedirs should receive the parent directory of the upload destination."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        file_data = _make_file_data("subdir/nested/report.pdf")
        sftp_uploader.run(path=local_file, file_data=file_data)

        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output/subdir/nested"
        assert mock_sftp_client.makedirs.call_args[1] == {"exist_ok": True}

    def test_makedirs_called_before_upload(self, sftp_uploader, mock_sftp_client, tmp_path):
        """makedirs must execute before upload — ordering matters on a real FS."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        call_order = []
        mock_sftp_client.makedirs.side_effect = lambda *a, **kw: call_order.append("makedirs")
        mock_sftp_client.upload.side_effect = lambda **kw: call_order.append("upload")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("deep/path/file.txt"))

        assert call_order == ["makedirs", "upload"]

    def test_flat_file_still_calls_makedirs(self, sftp_uploader, mock_sftp_client, tmp_path):
        """Even a file with no subdirectories should call makedirs (on the base dir)."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("flat_file.pdf"))

        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output"

    def test_upload_receives_correct_paths(self, sftp_uploader, mock_sftp_client, tmp_path):
        """upload should still receive the correct local and remote paths."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        sftp_uploader.run(path=local_file, file_data=_make_file_data("subdir/file.txt"))

        mock_sftp_client.upload.assert_called_once()
        call_kwargs = mock_sftp_client.upload.call_args[1]
        assert call_kwargs["lpath"] == str(local_file.resolve())
        assert "subdir/file.txt.json" in call_kwargs["rpath"]

    def test_error_propagated_after_makedirs(self, sftp_uploader, mock_sftp_client, tmp_path):
        """If upload fails, the error should still propagate via wrap_error."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        mock_sftp_client.upload.side_effect = IOError("SFTP write failed")

        with pytest.raises(Exception):
            sftp_uploader.run(path=local_file, file_data=_make_file_data("dir/file.txt"))

        # makedirs was still called before the failure
        mock_sftp_client.makedirs.assert_called_once()


class TestSftpUploaderMakeDirsAsync:
    """Verify the async path delegates to run() (which has the makedirs fix).

    SFTPFileSystem.async_impl is False, so the pipeline never calls run_async()
    for SFTP. But we override it to delegate to run() so the fix is present
    even if someone calls run_async() directly.
    """

    @pytest.mark.asyncio
    async def test_run_async_delegates_to_run(self, sftp_uploader, mock_sftp_client, tmp_path):
        """run_async should delegate to run(), inheriting the makedirs call."""
        local_file = tmp_path / "output.json"
        local_file.write_text("{}")

        await sftp_uploader.run_async(
            path=local_file, file_data=_make_file_data("nested/dir/file.txt")
        )

        # makedirs is called because run_async delegates to run()
        mock_sftp_client.makedirs.assert_called_once()
        parent_path = mock_sftp_client.makedirs.call_args[0][0]
        assert parent_path == "data/output/nested/dir"
        assert mock_sftp_client.makedirs.call_args[1] == {"exist_ok": True}
        mock_sftp_client.upload.assert_called_once()


# ---------------------------------------------------------------------------
# Host-key verification (SAP grounding compatibility)
# ---------------------------------------------------------------------------

import paramiko  # noqa: E402
from pydantic import ValidationError  # noqa: E402


def _openssh_public_key(kind: str) -> str:
    """Return a real OpenSSH public key line, e.g. 'ssh-ed25519 AAAA...'."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa

    if kind == "ssh-ed25519":
        pub = ed25519.Ed25519PrivateKey.generate().public_key()
    elif kind == "ssh-rsa":
        pub = rsa.generate_private_key(public_exponent=65537, key_size=2048).public_key()
    elif kind == "ecdsa":
        pub = ec.generate_private_key(ec.SECP256R1()).public_key()
    else:
        raise ValueError(kind)
    return pub.public_bytes(
        serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
    ).decode()


# Generate real keys once for the whole module (RSA generation is comparatively slow).
ED25519_LINE = _openssh_public_key("ssh-ed25519")
ED25519_BLOB = ED25519_LINE.split()[1]
RSA_LINE = _openssh_public_key("ssh-rsa")
RSA_BLOB = RSA_LINE.split()[1]
ECDSA_LINE = _openssh_public_key("ecdsa")
ECDSA_BLOB = ECDSA_LINE.split()[1]


class TestParseHostPublicKey:
    """Key type is auto-detected from the blob; multiple input forms accepted."""

    @pytest.mark.parametrize(
        ("value", "expected_type", "expected_blob"),
        [
            (ED25519_BLOB, "ssh-ed25519", ED25519_BLOB),  # bare blob
            (RSA_BLOB, "ssh-rsa", RSA_BLOB),
            (ED25519_LINE, "ssh-ed25519", ED25519_BLOB),  # ".pub" line
            (RSA_LINE, "ssh-rsa", RSA_BLOB),
            (f"host.example.com {ED25519_LINE}", "ssh-ed25519", ED25519_BLOB),  # known_hosts
            (f"[host.example.com]:2222 {RSA_LINE}", "ssh-rsa", RSA_BLOB),  # ssh-keyscan w/ port
            (f"  {ED25519_LINE}\n", "ssh-ed25519", ED25519_BLOB),  # surrounding whitespace
        ],
    )
    def test_autodetects_type_and_blob(self, value, expected_type, expected_blob):
        key_type, blob = parse_host_public_key(value)
        assert key_type == expected_type
        assert blob == expected_blob

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="empty"):
            parse_host_public_key("   ")

    def test_non_key_garbage_raises(self):
        with pytest.raises(ValueError, match="recognizable"):
            parse_host_public_key("this is not a key at all")

    def test_unsupported_type_raises_with_type_name(self):
        with pytest.raises(ValueError, match="Unsupported.*ecdsa"):
            parse_host_public_key(ECDSA_BLOB)

    def test_bad_base64_alone_raises(self):
        # Valid-looking token but not a real SSH key blob.
        with pytest.raises(ValueError):
            parse_host_public_key("AAAA")


class TestBuildHostKey:
    """Reconstructed paramiko key advertises the same algorithm we detected."""

    @pytest.mark.parametrize(
        ("blob", "expected_name"),
        [(ED25519_BLOB, "ssh-ed25519"), (RSA_BLOB, "ssh-rsa")],
    )
    def test_build_matches_type(self, blob, expected_name):
        key_type, key_b64 = parse_host_public_key(blob)
        key = _build_host_key(key_type, key_b64)
        assert key.get_name() == expected_name
        # Round-trips back to the same public blob.
        assert key.get_base64() == blob

    def test_unsupported_type_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            _build_host_key("ecdsa-sha2-nistp256", ECDSA_BLOB)


def _make_connection_config(host_public_key=None, host="localhost", port=2222):
    return SftpConnectionConfig(
        username="foo",
        host=host,
        port=port,
        access_config=SftpAccessConfig(password="pass"),
        host_public_key=host_public_key,
    )


class TestHostPublicKeyValidator:
    """Config validation fails fast on an unparsable / unsupported host key."""

    def test_none_is_allowed(self):
        cfg = _make_connection_config(host_public_key=None)
        assert cfg.host_public_key is None

    def test_valid_key_accepted(self):
        cfg = _make_connection_config(host_public_key=ED25519_LINE)
        assert cfg.host_public_key == ED25519_LINE

    def test_garbage_rejected(self):
        with pytest.raises(ValidationError):
            _make_connection_config(host_public_key="not-a-real-key")

    def test_unsupported_type_rejected(self):
        with pytest.raises(ValidationError):
            _make_connection_config(host_public_key=ECDSA_BLOB)

    def test_host_public_key_not_leaked_into_ssh_kwargs(self):
        # host_public_key is not a paramiko connect kwarg; it must not be passed
        # through get_access_config (which feeds fsspec/paramiko).
        cfg = _make_connection_config(host_public_key=ED25519_LINE)
        assert "host_public_key" not in cfg.get_access_config()


# --- get_client fakes ------------------------------------------------------


class _FakeHostKeys:
    def __init__(self):
        self.added = []

    def add(self, name, key_type, key):
        self.added.append((name, key_type, key))


class _FakeSSHClient:
    def __init__(self):
        self._host_keys = _FakeHostKeys()
        self.policy = None
        self.connected_with = None
        self.closed = False

    def get_host_keys(self):
        return self._host_keys

    def set_missing_host_key_policy(self, policy):
        self.policy = policy

    def connect(self, host, **kwargs):
        self.connected_with = (host, kwargs)

    def open_sftp(self):
        return object()

    def close(self):
        self.closed = True


class _FakeSFTPFileSystem:
    """Stand-in for fsspec's SFTPFileSystem __init__/_connect contract."""

    def __init__(self, skip_instance_cache=False, host=None, **ssh_kwargs):
        self.skip_instance_cache = skip_instance_cache
        self.host = host
        self.ssh_kwargs = ssh_kwargs
        self._connect()

    def _connect(self):
        # Base (unpinned) behavior: mimic fsspec creating its own client.
        self.client = _FakeSSHClient()
        self.ftp = self.client.open_sftp()


class TestGetClientUnpinned:
    """No host key -> connect but warn that identity is unverified."""

    def test_warns_and_connects(self, monkeypatch, caplog):
        import logging

        monkeypatch.setattr(
            "fsspec.get_filesystem_class", lambda protocol: _FakeSFTPFileSystem
        )
        cfg = _make_connection_config(host_public_key=None)

        with (
            caplog.at_level(logging.WARNING, logger="unstructured_ingest"),
            cfg.get_client(protocol="sftp") as client,
        ):
            assert isinstance(client, _FakeSFTPFileSystem)
        assert any("without a pinned host key" in r.getMessage() for r in caplog.records)
        # Connection was closed on context exit.
        assert client.client.closed is True


class TestGetClientPinned:
    """Host key present -> pin it and use RejectPolicy (no AutoAddPolicy)."""

    def _run(self, monkeypatch, host_public_key, host, port):
        monkeypatch.setattr(
            "fsspec.get_filesystem_class", lambda protocol: _FakeSFTPFileSystem
        )
        created = []

        def _factory():
            client = _FakeSSHClient()
            created.append(client)
            return client

        monkeypatch.setattr(paramiko, "SSHClient", _factory)

        cfg = _make_connection_config(host_public_key=host_public_key, host=host, port=port)
        with cfg.get_client(protocol="sftp") as client:
            pass
        assert created, "expected our own paramiko.SSHClient to be built"
        return created[0], client

    def test_rejectpolicy_and_pins_key(self, monkeypatch):
        ssh_client, fs = self._run(monkeypatch, ED25519_LINE, "example.com", 2222)
        # RejectPolicy (never AutoAddPolicy) so a wrong/unknown key is refused.
        assert isinstance(ssh_client.policy, paramiko.RejectPolicy)
        # Exactly one pinned key, of the auto-detected type.
        assert len(ssh_client._host_keys.added) == 1
        entry_name, key_type, key = ssh_client._host_keys.added[0]
        assert key_type == "ssh-ed25519"
        assert key.get_name() == "ssh-ed25519"
        # Non-default port -> "[host]:port" known_hosts naming.
        assert entry_name == "[example.com]:2222"
        # Connected to the right host with password auth kwargs.
        connect_host, connect_kwargs = ssh_client.connected_with
        assert connect_host == "example.com"
        assert connect_kwargs["password"] == "pass"
        assert connect_kwargs["look_for_keys"] is False
        # Closed on exit.
        assert ssh_client.closed is True

    def test_default_port_uses_plain_host_entry(self, monkeypatch):
        ssh_client, _ = self._run(monkeypatch, RSA_LINE, "sftp.example.com", 22)
        entry_name, key_type, _ = ssh_client._host_keys.added[0]
        assert key_type == "ssh-rsa"
        assert entry_name == "sftp.example.com"  # no brackets for port 22
