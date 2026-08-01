"""Unit tests for the GCS connector's wrap_error redaction."""

from __future__ import annotations

import pytest
from pydantic import Secret

from unstructured_ingest.error import ProviderError, UserError
from unstructured_ingest.error import ValueError as IngestValueError
from unstructured_ingest.processes.connectors.fsspec.gcs import (
    GcsAccessConfig,
    GcsConnectionConfig,
)

_SECRET = "leaked-gcs-message-abc123XYZ"


@pytest.fixture
def connection_config() -> GcsConnectionConfig:
    return GcsConnectionConfig(
        access_config=Secret(GcsAccessConfig()),
        remote_url="gs://test",
    )


def test_gcs_wrap_error_client_error_redacts_message(
    connection_config: GcsConnectionConfig,
):
    pytest.importorskip("gcsfs")
    from gcsfs.retry import HttpError

    error = HttpError({"code": 403, "message": f"forbidden {_SECRET}"})

    with pytest.raises(UserError) as exc_info:
        connection_config.wrap_error(error)

    assert _SECRET not in str(exc_info.value)


def test_gcs_wrap_error_server_error_redacts_message(
    connection_config: GcsConnectionConfig,
):
    pytest.importorskip("gcsfs")
    from gcsfs.retry import HttpError

    error = HttpError({"code": 500, "message": f"server error {_SECRET}"})

    with pytest.raises(ProviderError) as exc_info:
        connection_config.wrap_error(error)

    assert _SECRET not in str(exc_info.value)


def test_gcs_wrap_error_file_not_found_redacts_message(
    connection_config: GcsConnectionConfig,
):
    pytest.importorskip("gcsfs")

    error = FileNotFoundError(f"gs://bucket/{_SECRET}")

    with pytest.raises(UserError) as exc_info:
        connection_config.wrap_error(error)

    message = str(exc_info.value)
    assert _SECRET not in message
    assert "File not found" in message


def test_gcs_wrap_error_forbidden_uses_static_message(
    connection_config: GcsConnectionConfig,
):
    pytest.importorskip("gcsfs")

    error = OSError(f"Forbidden: gs://bucket/{_SECRET}")

    with pytest.raises(UserError) as exc_info:
        connection_config.wrap_error(error)

    message = str(exc_info.value)
    assert _SECRET not in message
    # Actionable signal survives without the raw text.
    assert "forbidden" in message.lower()


def test_gcs_wrap_error_bad_request_uses_static_message(
    connection_config: GcsConnectionConfig,
):
    pytest.importorskip("gcsfs")

    # gcs.wrap_error matches the connector's own error.ValueError (the import at
    # gcs.py:14 shadows the builtin), so the branch only fires for that type.
    error = IngestValueError(f"Bad Request: gs://bucket/{_SECRET}")

    with pytest.raises(UserError) as exc_info:
        connection_config.wrap_error(error)

    message = str(exc_info.value)
    assert _SECRET not in message
    # Actionable signal survives without the raw text.
    assert "bad request" in message.lower()
