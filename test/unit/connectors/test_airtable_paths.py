from unittest.mock import Mock

import pytest

from unstructured_ingest.error import ValueError
from unstructured_ingest.processes.connectors.airtable import (
    AirtableAccessConfig,
    AirtableConnectionConfig,
    AirtableIndexer,
    AirtableIndexerConfig,
    AirtableTableMeta,
)


@pytest.mark.parametrize("path", ["base", "base/"])
def test_base_paths_enumerate_tables(path):
    indexer = AirtableIndexer(
        connection_config=AirtableConnectionConfig(
            access_config=AirtableAccessConfig(personal_access_token="token")
        ),
        index_config=AirtableIndexerConfig(list_of_paths=[path]),
    )
    tables = [AirtableTableMeta(base_id="base", table_id="table")]
    indexer.get_base_tables_meta = Mock(return_value=tables)
    assert indexer.get_meta_from_list() == tables
    indexer.get_base_tables_meta.assert_called_once_with(base_id="base")


@pytest.mark.parametrize(
    "path", ["base/table", "base/table/", "base/table/view", "base/table/view/"]
)
def test_table_and_view_paths_accept_terminal_separator(path):
    indexer = AirtableIndexer(
        connection_config=AirtableConnectionConfig(
            access_config=AirtableAccessConfig(personal_access_token="token")
        ),
        index_config=AirtableIndexerConfig(list_of_paths=[path]),
    )
    assert indexer.get_meta_from_list() == [
        AirtableTableMeta(
            base_id="base", table_id="table", view_id="view" if "view" in path else None
        )
    ]


@pytest.mark.parametrize(
    "path", ["", "/", "/base", "base//view", "base//", "base/table//", "base/table/view/extra"]
)
def test_missing_or_extra_ids_are_rejected(path):
    with pytest.raises(ValueError):
        AirtableIndexerConfig(list_of_paths=[path])
