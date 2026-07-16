import logging

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.processes.connectors.duckdb.duckdb import (
    DuckDBConnectionConfig,
    DuckDBUploader,
    DuckDBUploaderConfig,
)

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _uploader() -> DuckDBUploader:
    connection_config = DuckDBConnectionConfig(database="test.db")
    return DuckDBUploader(
        upload_config=DuckDBUploaderConfig(),
        connection_config=connection_config,
    )


def test_precheck_redacts_provider_exception(
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    uploader = _uploader()
    uploader.connection_config = mocker.MagicMock()
    uploader.connection_config.get_cursor.side_effect = RuntimeError(SECRET)

    with (
        caplog.at_level(logging.ERROR, logger="unstructured_ingest"),
        pytest.raises(DestinationConnectionError) as exc_info,
    ):
        uploader.precheck()

    assert SECRET not in str(exc_info.value)
    assert "hunter2" not in str(exc_info.value)
    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text
