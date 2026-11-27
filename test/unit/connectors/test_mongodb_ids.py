from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from unstructured_ingest.data_types.file_data import file_data_from_dict
from unstructured_ingest.processes.connectors.mongodb import (
    MongoDBConnectionConfig,
    MongoDBDownloader,
    MongoDBDownloaderConfig,
    MongoDBIndexer,
    MongoDBIndexerConfig,
)

pytest.importorskip("pymongo")

from bson.objectid import ObjectId  # noqa: E402


def _run_source(monkeypatch, tmp_path, stored_ids):
    collection = MagicMock()
    collection.distinct.return_value = list(stored_ids)
    # Match on type as well as value, like MongoDB does for strings vs. numbers/ObjectIds
    collection.find.side_effect = lambda query: [
        {"_id": stored, "text": "hello"}
        for stored in stored_ids
        if any(type(stored) is type(q) and stored == q for q in query["_id"]["$in"])
    ]
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = collection

    @contextmanager
    def get_client(self):
        yield client

    monkeypatch.setattr(MongoDBConnectionConfig, "get_client", get_client)
    connection_config = MongoDBConnectionConfig()
    indexer = MongoDBIndexer(
        connection_config=connection_config,
        index_config=MongoDBIndexerConfig(database="db", collection="col"),
    )
    downloader = MongoDBDownloader(
        connection_config=connection_config,
        download_config=MongoDBDownloaderConfig(download_dir=tmp_path),
    )
    responses = []
    for batch in indexer.run():
        # the pipeline hands file data to the downloader through JSON
        dumped = batch.model_dump(mode="json")
        # ObjectId collections keep the same batch file data as before
        assert ("id_values" in dumped["additional_metadata"]) != isinstance(stored_ids[0], ObjectId)
        file_data = file_data_from_dict(dumped)
        responses.extend(downloader.run(file_data=file_data))
    return responses


@pytest.mark.parametrize(
    "stored_ids",
    [
        ["user-1", "user-2"],
        [1, 2],
        # a string that happens to look like an ObjectId must stay a string
        ["507f1f77bcf86cd799439011"],
        [ObjectId("507f1f77bcf86cd799439011"), ObjectId("507f1f77bcf86cd799439012")],
    ],
)
def test_download_keeps_the_id_type(monkeypatch, tmp_path, stored_ids):
    responses = _run_source(monkeypatch, tmp_path, stored_ids)

    assert [r["file_data"].identifier for r in responses] == [str(i) for i in stored_ids]
    for response in responses:
        assert response["path"].read_text(encoding="utf8") == "hello"
        assert response["file_data"].additional_metadata == {"database": "db", "collection": "col"}


def test_string_id_with_path_separators_stays_in_download_dir(monkeypatch, tmp_path):
    download_dir = tmp_path / "downloads"
    responses = _run_source(monkeypatch, download_dir, ["../escape", "a/b", "a_b"])

    paths = [r["path"] for r in responses]
    assert all(path.parent == download_dir for path in paths)
    assert len(set(paths)) == 3
