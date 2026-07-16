import logging

import pytest

from unstructured_ingest.error import ProviderError, UserAuthError, UserError
from unstructured_ingest.processes.connectors.databricks.volumes_native import (
    DatabricksNativeVolumesAccessConfig,
    DatabricksNativeVolumesConnectionConfig,
)

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _connection_config() -> DatabricksNativeVolumesConnectionConfig:
    return DatabricksNativeVolumesConnectionConfig(
        access_config=DatabricksNativeVolumesAccessConfig(token=SECRET),
        host="https://example.databricks.com",
    )


def test_wrap_error_value_auth_redacts():
    pytest.importorskip("databricks.sdk")
    config = _connection_config()
    wrapped = config.wrap_error(ValueError(f"auth: {SECRET}"))

    assert isinstance(wrapped, UserAuthError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_databricks_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[403]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, UserAuthError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_provider_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[500]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, ProviderError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_user_error_redacts():
    pytest.importorskip("databricks.sdk")
    from databricks.sdk.errors.platform import STATUS_CODE_MAPPING

    error_cls = STATUS_CODE_MAPPING[400]
    wrapped = _connection_config().wrap_error(error_cls(SECRET))

    assert isinstance(wrapped, UserError)
    assert SECRET not in str(wrapped)
    assert "hunter2" not in str(wrapped)


def test_wrap_error_unhandled_log_redacts(caplog: pytest.LogCaptureFixture):
    # A non-Databricks, non-auth ValueError falls through to the unhandled
    # log path and is returned raw; the log line must still be redacted.
    pytest.importorskip("databricks.sdk")
    with caplog.at_level(logging.ERROR, logger="unstructured_ingest"):
        _connection_config().wrap_error(RuntimeError(SECRET))

    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text
