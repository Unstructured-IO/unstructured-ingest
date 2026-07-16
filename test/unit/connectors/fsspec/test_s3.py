import logging

import pytest

from unstructured_ingest.error import ProviderError, UserAuthError, UserError
from unstructured_ingest.processes.connectors.fsspec.s3 import S3ConnectionConfig

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _connection_config() -> S3ConnectionConfig:
    return S3ConnectionConfig(remote_url="s3://bucket/path", anonymous=True)


def _botocore_style_error(http_code: int) -> Exception:
    cause = Exception("boto cause")
    cause.response = {
        "ResponseMetadata": {"HTTPStatusCode": http_code},
        "Error": {"Message": SECRET},
    }
    outer = Exception(SECRET)
    outer.__cause__ = cause
    return outer


def test_wrap_error_permission_error_redacts():
    wrapped = _connection_config().wrap_error(PermissionError(SECRET))

    assert isinstance(wrapped, UserAuthError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_botocore_4xx_redacts():
    wrapped = _connection_config().wrap_error(_botocore_style_error(403))

    assert isinstance(wrapped, UserError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_botocore_5xx_redacts():
    wrapped = _connection_config().wrap_error(_botocore_style_error(503))

    assert isinstance(wrapped, ProviderError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_unhandled_log_redacts(caplog: pytest.LogCaptureFixture):
    # No __cause__, not Permission/FileNotFound: falls through to the unhandled
    # log path and is returned raw; the log line must still be redacted.
    with caplog.at_level(logging.ERROR, logger="unstructured_ingest"):
        _connection_config().wrap_error(RuntimeError(SECRET))

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text
