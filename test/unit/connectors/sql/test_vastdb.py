import logging

import pytest
from pytest_mock import MockerFixture

from unstructured_ingest.error import DestinationConnectionError
from unstructured_ingest.processes.connectors.sql.vastdb import (
    VastdbConnectionConfig,
    VastdbIndexer,
    VastdbIndexerConfig,
)

SECRET = "SECRETpassword=hunter2 key=AKIAEXAMPLE"


def _indexer() -> VastdbIndexer:
    connection_config = VastdbConnectionConfig(
        vastdb_bucket="bucket",
        vastdb_schema="schema",
    )
    return VastdbIndexer(
        connection_config=connection_config,
        index_config=VastdbIndexerConfig(table_name="elements", id_column="id"),
    )


def test_precheck_redacts_provider_exception(
    mocker: MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    indexer = _indexer()
    indexer.connection_config = mocker.MagicMock()
    indexer.connection_config.get_table.side_effect = RuntimeError(SECRET)

    with (
        caplog.at_level(logging.ERROR, logger="unstructured_ingest"),
        pytest.raises(DestinationConnectionError) as exc_info,
    ):
        indexer.precheck()

    assert SECRET not in str(exc_info.value)
    assert "hunter2" not in str(exc_info.value)
    assert SECRET not in caplog.text
    assert "hunter2" not in caplog.text
