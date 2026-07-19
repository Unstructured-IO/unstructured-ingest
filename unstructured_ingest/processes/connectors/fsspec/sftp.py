from __future__ import annotations

import base64
import binascii
import contextlib
import hashlib
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, Any, Generator, Optional
from urllib.parse import urlparse

from pydantic import Field, Secret, model_validator

from unstructured_ingest.data_types.file_data import FileData, FileDataSourceMetadata
from unstructured_ingest.logger import logger
from unstructured_ingest.processes.connector_registry import (
    DestinationRegistryEntry,
    LocationShape,
    SourceRegistryEntry,
)
from unstructured_ingest.processes.connectors.fsspec.fsspec import (
    FsspecAccessConfig,
    FsspecConnectionConfig,
    FsspecDownloader,
    FsspecDownloaderConfig,
    FsspecIndexer,
    FsspecIndexerConfig,
    FsspecUploader,
    FsspecUploaderConfig,
)
from unstructured_ingest.processes.utils.blob_storage import (
    BlobStoreUploadStager,
    BlobStoreUploadStagerConfig,
)
from unstructured_ingest.utils.dep_check import requires_dependencies

if TYPE_CHECKING:
    from fsspec.implementations.sftp import SFTPFileSystem

CONNECTOR_TYPE = "sftp"


def _strip_leading_slash(path: str) -> str:
    """Strip one leading slash, preserving double-slash absolute path indicators.
    Example: sftp://host//home → /home (absolute), sftp://host/data → data (relative)."""
    return path[1:] if path.startswith("/") else path


# Host key types supported for server host-key verification.
SUPPORTED_HOST_KEY_TYPES: tuple[str, ...] = ("ssh-ed25519", "ssh-rsa")


def _extract_ssh_key_type(raw: bytes) -> str:
    """Read the algorithm name (first length-prefixed field) from an SSH key blob.

    The type is derived from the key material itself, so no separate type input
    is required.
    """
    if len(raw) < 4:
        raise ValueError("host key blob is too short to contain a key type")
    length = int.from_bytes(raw[:4], "big")
    if length <= 0 or len(raw) < 4 + length:
        raise ValueError("host key blob is malformed or truncated")
    try:
        return raw[4 : 4 + length].decode("ascii")
    except UnicodeDecodeError as e:
        raise ValueError(f"host key blob has a non-ASCII key type: {e}") from e


def _iter_ssh_wire_fields(raw: bytes) -> list[bytes]:
    """Split an SSH wire blob into its length-prefixed fields.

    Requires the fields to consume the blob exactly; raises ``ValueError`` on a
    length prefix that overruns the buffer, a truncated prefix, or trailing bytes.
    """
    fields: list[bytes] = []
    offset = 0
    total = len(raw)
    while offset < total:
        if total - offset < 4:
            raise ValueError("host key blob is malformed (truncated length prefix)")
        length = int.from_bytes(raw[offset : offset + 4], "big")
        offset += 4
        if total - offset < length:
            raise ValueError("host key blob is malformed (field overruns the blob)")
        fields.append(raw[offset : offset + length])
        offset += length
    return fields


# Number of wire fields per supported key type: the algorithm name plus its key
# parameters (ed25519 -> name + 32-byte public key; rsa -> name + e + n).
_EXPECTED_FIELD_COUNTS = {"ssh-ed25519": 2, "ssh-rsa": 3}


def _validate_host_key_wire_format(raw: bytes, key_type: str) -> None:
    """Validate the full key wire format, not just the algorithm prefix.

    ``_extract_ssh_key_type`` only reads the leading algorithm field, so a blob
    with a supported prefix but a truncated/garbage body would otherwise pass
    config validation and fail only at connect. This closes that gap.

    Boundary: this checks *structure* only (field framing, field count, and the
    fixed 32-byte ed25519 length), not cryptographic validity (e.g. that ssh-rsa
    ``e``/``n`` are canonical/usable). Full validation would require building the
    key via paramiko, which we avoid at config time so constructing
    ``SftpConnectionConfig`` doesn't need the optional ``sftp`` extra; a
    structurally valid but bogus key is caught later at connect (precheck).
    """
    fields = _iter_ssh_wire_fields(raw)
    expected = _EXPECTED_FIELD_COUNTS[key_type]
    if len(fields) != expected:
        raise ValueError(
            f"malformed {key_type} host key: expected {expected} wire fields, got {len(fields)}"
        )
    if key_type == "ssh-ed25519" and len(fields[1]) != 32:
        raise ValueError(
            f"malformed ssh-ed25519 host key: public key must be 32 bytes, got {len(fields[1])}"
        )


def parse_host_public_key(value: str) -> tuple[str, str]:
    """Return ``(key_type, base64_blob)`` for a server host public key string.

    Accepts any of the common representations, so the caller never has to declare
    the key type:

      - a bare base64 blob:              ``"AAAAC3Nza..."``
      - an OpenSSH ``.pub`` line:        ``"ssh-ed25519 AAAAC3Nza... comment"``
      - an ssh-keyscan/known_hosts line: ``"host ssh-ed25519 AAAAC3Nza..."``

    The key type is auto-detected from the decoded blob (not from any surrounding
    label), which is authoritative and avoids a label/key mismatch. Raises
    ``ValueError`` for anything that does not contain a supported, decodable SSH
    public key.
    """
    tokens = value.split()
    if not tokens:
        raise ValueError("host_public_key is empty")

    unsupported: list[str] = []
    for token in tokens:
        try:
            raw = base64.b64decode(token, validate=True)
        except (binascii.Error, ValueError):
            # Not a base64 field (e.g. the "ssh-ed25519" label or a hostname).
            continue
        try:
            key_type = _extract_ssh_key_type(raw)
        except ValueError:
            continue
        if key_type in SUPPORTED_HOST_KEY_TYPES:
            # Fail fast on a truncated/garbage body, not later at connect.
            _validate_host_key_wire_format(raw, key_type)
            return key_type, token
        unsupported.append(key_type)

    if unsupported:
        raise ValueError(
            f"Unsupported SFTP host key type(s) {unsupported}; "
            f"supported types are {list(SUPPORTED_HOST_KEY_TYPES)}"
        )
    raise ValueError(
        "host_public_key does not contain a recognizable base64-encoded SSH public key "
        "(expected a bare blob, an OpenSSH '.pub' line, or an ssh-keyscan/known_hosts line)"
    )


def _build_host_key(key_type: str, key_b64: str):
    """Reconstruct a paramiko key object from a base64 host key blob."""
    import paramiko

    data = base64.b64decode(key_b64)
    if key_type == "ssh-ed25519":
        return paramiko.Ed25519Key(data=data)
    if key_type == "ssh-rsa":
        return paramiko.RSAKey(data=data)
    # Defensive: parse_host_public_key already gates the supported set.
    raise ValueError(f"Unsupported SFTP host key type: {key_type!r}")


# Patch hashlib.md5 for FIPS-enabled OpenSSL (common in Kubernetes).
# Paramiko uses MD5 solely for logging human-readable host key fingerprints,
# not for any cryptographic purpose (SSH security uses Ed25519/SHA-256).
# This flag tells OpenSSL the MD5 call is non-cryptographic, which is safe.
_original_md5 = hashlib.md5


def _fips_safe_md5(data=b"", **kwargs):
    kwargs.setdefault("usedforsecurity", False)
    return _original_md5(data, **kwargs)


hashlib.md5 = _fips_safe_md5


class SftpIndexerConfig(FsspecIndexerConfig):
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        _, ext = os.path.splitext(self.remote_url)
        parsed_url = urlparse(self.remote_url)
        if ext:
            parent_path = Path(parsed_url.path).parent.as_posix()
            self.path_without_protocol = _strip_leading_slash(parent_path)
        else:
            self.path_without_protocol = _strip_leading_slash(parsed_url.path)


class SftpAccessConfig(FsspecAccessConfig):
    password: str = Field(description="Password for sftp connection")


class SftpConnectionConfig(FsspecConnectionConfig):
    supported_protocols: list[str] = Field(default_factory=lambda: ["sftp"], init=False)
    access_config: Secret[SftpAccessConfig]
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    username: str = Field(description="Username for sftp connection")
    host: Optional[str] = Field(default=None, description="Hostname for sftp connection")
    port: int = Field(default=22, description="Port for sftp connection")
    look_for_keys: bool = Field(
        default=False, description="Whether to search for private key files in ~/.ssh/"
    )
    allow_agent: bool = Field(default=False, description="Whether to connect to the SSH agent.")
    host_public_key: Optional[str] = Field(
        default=None,
        description=(
            "Server host public key used to verify the server's identity. Accepts a "
            "base64 blob (e.g. 'AAAAC3Nza...'), an OpenSSH '.pub' line, or an "
            "ssh-keyscan/known_hosts line; the key type (ssh-ed25519 / ssh-rsa) is "
            "auto-detected from the key. When omitted, the server identity is NOT "
            "verified and a warning is logged."
        ),
    )

    @model_validator(mode="after")
    def _validate_host_public_key(self) -> "SftpConnectionConfig":
        # Fail fast at config time if a host key is supplied but unparsable /
        # unsupported. A well-formed but wrong key is caught later at connect
        # (precheck) as a host-key mismatch.
        if self.host_public_key is not None:
            parse_host_public_key(self.host_public_key)
        return self

    def get_access_config(self) -> dict[str, Any]:
        access_config = {
            "username": self.username,
            "host": self.host,
            "port": self.port,
            "look_for_keys": self.look_for_keys,
            "allow_agent": self.allow_agent,
            "password": self.access_config.get_secret_value().password,
        }
        return access_config

    @contextmanager
    @requires_dependencies(["paramiko", "fsspec"], extras="sftp")
    def get_client(self, protocol: str) -> Generator["SFTPFileSystem", None, None]:
        # The paramiko.SSHClient() client that's opened by the SFTPFileSystem
        # never gets closed so explicitly adding that as part of this context manager.
        # skip_instance_cache=True is required to prevent fsspec from returning a cached
        # instance whose SSH connection was closed by a previous context manager exit.
        from fsspec import get_filesystem_class

        fs_cls = get_filesystem_class(protocol)
        access_config = self.get_access_config()

        if self.host_public_key is None:
            # No pinned host key: connect but do NOT verify the server identity.
            # fsspec's SFTPFileSystem._connect uses paramiko.AutoAddPolicy(), so any
            # host key the server presents is accepted. Warn so this is a deliberate,
            # visible choice rather than a silent security gap.
            logger.warning(
                "Connecting to SFTP host %r without a pinned host key; the server "
                "identity is NOT verified (vulnerable to MITM). Provide `host_public_key` "
                "to enable host-key verification.",
                self.host,
            )
            client: SFTPFileSystem = fs_cls(skip_instance_cache=True, **access_config)
            try:
                yield client
            finally:
                client.client.close()
            return

        # Pinned host-key path: build the paramiko client ourselves so we can
        # pin the expected host key and use RejectPolicy.
        # fsspec's SFTPFileSystem._connect hardcodes AutoAddPolicy and offers no
        # hook, so we override _connect to verify the server before trusting it.
        import paramiko

        key_type, key_b64 = parse_host_public_key(self.host_public_key)
        host_key = _build_host_key(key_type, key_b64)

        class _VerifiedSFTPFileSystem(fs_cls):
            def _connect(self):
                ssh_client = paramiko.SSHClient()
                port = self.ssh_kwargs.get("port", 22)
                # GOTCHA: paramiko looks up known host keys under "[host]:port" for
                # non-default ports (the OpenSSH known_hosts convention); register
                # under the same name or verification silently never matches.
                entry_name = self.host if port == 22 else f"[{self.host}]:{port}"
                ssh_client.get_host_keys().add(entry_name, key_type, host_key)
                # Reject (never auto-add) an unknown or mismatched server key.
                ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
                ssh_client.connect(self.host, **self.ssh_kwargs)
                self.client = ssh_client
                self.ftp = ssh_client.open_sftp()

        client = _VerifiedSFTPFileSystem(skip_instance_cache=True, **access_config)
        try:
            yield client
        finally:
            client.client.close()


@dataclass
class SftpIndexer(FsspecIndexer):
    connection_config: SftpConnectionConfig
    index_config: SftpIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def __post_init__(self):
        super().__post_init__()
        parsed_url = urlparse(self.index_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    def precheck(self) -> None:
        self.log_operation_start(
            "Connection validation",
            protocol=self.index_config.protocol,
            path=self.index_config.path_without_protocol,
        )

        try:
            with self.connection_config.get_client(protocol=self.index_config.protocol) as client:
                files = client.ls(path=self.index_config.path_without_protocol, detail=True)
                valid_files = [x.get("name") for x in files if x.get("type") == "file"]
                if not valid_files:
                    self.log_operation_complete("Connection validation", count=0)
                    return
                file_to_sample = valid_files[0]
                self.log_debug(f"attempting to make HEAD request for file: {file_to_sample}")
                client.head(path=file_to_sample)

            self.log_connection_validated(
                connector_type=self.connector_type,
                endpoint=f"{self.index_config.protocol}://{self.index_config.path_without_protocol}",
            )

        except Exception as e:
            self.log_connection_failed(
                connector_type=self.connector_type,
                error=e,
                endpoint=f"{self.index_config.protocol}://{self.index_config.path_without_protocol}",
            )
            raise self.wrap_error(e=e)

    def get_metadata(self, file_info: dict) -> FileDataSourceMetadata:
        path = file_info["name"]
        date_created = str(file_info.get("time").timestamp()) if "time" in file_info else None
        date_modified = str(file_info.get("mtime").timestamp()) if "mtime" in file_info else None

        file_size = file_info.get("size") if "size" in file_info else None

        record_locator = {
            "protocol": self.index_config.protocol,
            "remote_file_path": self.index_config.remote_url,
        }
        return FileDataSourceMetadata(
            date_created=date_created,
            date_modified=date_modified,
            date_processed=str(time()),
            url=f"{self.index_config.protocol}://{path}",
            record_locator=record_locator,
            filesize_bytes=file_size,
        )


class SftpDownloaderConfig(FsspecDownloaderConfig):
    remote_url: str = Field(description="Remote fsspec URL formatted as `protocol://dir/path`")


@dataclass
class SftpDownloader(FsspecDownloader):
    protocol: str = "sftp"
    connection_config: SftpConnectionConfig
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    download_config: Optional[SftpDownloaderConfig] = field(default_factory=SftpDownloaderConfig)

    def __post_init__(self):
        super().__post_init__()
        parsed_url = urlparse(self.download_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port


class SftpUploaderConfig(FsspecUploaderConfig):
    def model_post_init(self, __context: Any) -> None:
        super().model_post_init(__context)
        parsed_url = urlparse(self.remote_url)
        self.path_without_protocol = _strip_leading_slash(parsed_url.path)


@dataclass
class SftpUploader(FsspecUploader):
    connector_type: str = CONNECTOR_TYPE
    connection_config: SftpConnectionConfig
    upload_config: SftpUploaderConfig = field(default=None)

    def __post_init__(self):
        super().__post_init__()
        parsed_url = urlparse(self.upload_config.remote_url)
        self.connection_config.host = parsed_url.hostname or self.connection_config.host
        self.connection_config.port = parsed_url.port or self.connection_config.port

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        path_str = str(path.resolve())
        upload_path = self.get_upload_path(file_data=file_data)
        self.log_upload_start(file_path=path_str, destination=upload_path.as_posix())
        try:
            with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
                # fsspec's SFTPFileSystem.put() bypasses the base class's mkdirs
                # (going straight to paramiko), so we create parent dirs explicitly.
                client.makedirs(upload_path.parent.as_posix(), exist_ok=True)
                client.upload(lpath=path_str, rpath=upload_path.as_posix())
        except Exception as e:
            self.log_error(
                "File upload failed",
                error=e,
                context={"file_path": path_str, "destination": upload_path.as_posix()},
            )
            raise self.wrap_error(e=e)
        self.log_upload_complete(file_path=path_str, destination=upload_path.as_posix())

    async def run_async(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        self.run(path=path, file_data=file_data, **kwargs)

    def precheck(self) -> None:
        self.log_operation_start("Connection validation", protocol=self.upload_config.protocol)

        try:
            with self.connection_config.get_client(protocol=self.upload_config.protocol) as client:
                upload_path = Path(self.upload_config.path_without_protocol) / "_empty"
                # Create parent directories if they don't exist
                client.makedirs(upload_path.parent.as_posix(), exist_ok=True)
                client.write_bytes(path=upload_path.as_posix(), value=b"")
                # Best-effort cleanup - don't fail if user lacks delete permissions
                with contextlib.suppress(Exception):
                    client.rm(path=upload_path.as_posix())

            self.log_connection_validated(
                connector_type=self.connector_type,
                endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
            )

        except Exception as e:
            self.log_connection_failed(
                connector_type=self.connector_type,
                error=e,
                endpoint=f"{self.upload_config.protocol}://{self.upload_config.path_without_protocol}",
            )
            raise self.wrap_error(e=e)


# sftp does not emit a per-record version, so emits_record_version stays False.
sftp_source_entry = SourceRegistryEntry(
    indexer=SftpIndexer,
    indexer_config=SftpIndexerConfig,
    downloader=SftpDownloader,
    downloader_config=SftpDownloaderConfig,
    connection_config=SftpConnectionConfig,
    location_shape=LocationShape.FSSPEC_URL,
    location_identity=("indexer_config.remote_url",),
)

sftp_destination_entry = DestinationRegistryEntry(
    uploader=SftpUploader,
    uploader_config=SftpUploaderConfig,
    connection_config=SftpConnectionConfig,
    upload_stager_config=BlobStoreUploadStagerConfig,
    upload_stager=BlobStoreUploadStager,
    location_shape=LocationShape.FSSPEC_URL,
    location_identity=("uploader_config.remote_url",),
)
