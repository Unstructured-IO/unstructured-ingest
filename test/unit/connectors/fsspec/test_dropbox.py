"""Unit tests for the Dropbox connector's wrap_error redaction."""

from __future__ import annotations

import pytest
from pydantic import Secret

from unstructured_ingest.error import ProviderError, UserAuthError, UserError
from unstructured_ingest.processes.connectors.fsspec.dropbox import (
    DropboxAccessConfig,
    DropboxConnectionConfig,
)

_SECRET = "leaked-dropbox-body-abc123XYZ"


@pytest.fixture
def connection_config() -> DropboxConnectionConfig:
    return DropboxConnectionConfig(
        access_config=Secret(DropboxAccessConfig(token="test-token")),
        remote_url="dropbox://test",
    )


def test_dropbox_wrap_error_auth_error_redacts_body(
    connection_config: DropboxConnectionConfig,
):
    pytest.importorskip("dropbox")
    from dropbox.exceptions import AuthError

    error = AuthError("req-123", f"auth failed {_SECRET}")

    with pytest.raises(UserAuthError) as exc_info:
        connection_config.wrap_error(error)

    assert _SECRET not in str(exc_info.value)


def test_dropbox_wrap_error_client_error_redacts_body(
    connection_config: DropboxConnectionConfig,
):
    pytest.importorskip("dropbox")
    from dropbox.exceptions import HttpError

    error = HttpError("req-123", 400, f"bad request {_SECRET}")

    wrapped = connection_config.wrap_error(error)

    assert isinstance(wrapped, UserError)
    assert _SECRET not in str(wrapped)


def test_dropbox_wrap_error_server_error_redacts_body(
    connection_config: DropboxConnectionConfig,
):
    pytest.importorskip("dropbox")
    from dropbox.exceptions import HttpError

    error = HttpError("req-123", 500, f"server exploded {_SECRET}")

    wrapped = connection_config.wrap_error(error)

    assert isinstance(wrapped, ProviderError)
    assert _SECRET not in str(wrapped)
