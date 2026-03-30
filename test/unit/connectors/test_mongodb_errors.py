from unittest.mock import MagicMock

import pytest
from pymongo.errors import (
    AutoReconnect,
    BulkWriteError,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from unstructured_ingest.error import (
    DestinationConnectionError,
    QuotaError,
    TimeoutError,
    WriteError,
)
from unstructured_ingest.processes.connectors.mongodb import (
    MongoDBConnectionConfig,
    MongoDBUploader,
    MongoDBUploaderConfig,
)


def _make_uploader():
    connection_config = MagicMock(spec=MongoDBConnectionConfig)
    connection_config.host = "test_host"
    upload_config = MagicMock(spec=MongoDBUploaderConfig)
    upload_config.record_id_key = "record_id"
    upload_config.database = "test_db"
    upload_config.collection = "test_collection"
    upload_config.batch_size = 100
    return MongoDBUploader(
        connection_config=connection_config,
        upload_config=upload_config,
    )


def _mock_client(uploader, collection_side_effect=None):
    mock_client = MagicMock()
    uploader.connection_config.get_client.return_value.__enter__ = MagicMock(
        return_value=mock_client
    )
    uploader.connection_config.get_client.return_value.__exit__ = MagicMock(return_value=False)
    mock_collection = mock_client.__getitem__("test_db").__getitem__("test_collection")
    # Make can_delete return False so we skip delete_by_record_id and go straight to insert
    mock_collection.list_indexes.return_value = []
    if collection_side_effect:
        mock_collection.insert_many.side_effect = collection_side_effect
    return mock_collection


class TestRunDataErrorHandling:
    def test_operation_failure_quota_raises_quota_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, OperationFailure("quota exceeded for writes"))

        with pytest.raises(QuotaError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_operation_failure_other_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, OperationFailure("some other failure"))

        with pytest.raises(DestinationConnectionError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_server_selection_timeout_raises_timeout_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, ServerSelectionTimeoutError("timeout"))

        with pytest.raises(TimeoutError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_bulk_write_error_raises_write_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, BulkWriteError({"writeErrors": [{"errmsg": "fail"}]}))

        with pytest.raises(WriteError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)

    def test_auto_reconnect_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        _mock_client(uploader, AutoReconnect("connection lost"))

        with pytest.raises(DestinationConnectionError):
            uploader.run_data(data=[{"key": "value"}], file_data=file_data)


class TestDeleteByRecordIdErrorHandling:
    def test_operation_failure_quota_raises_quota_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = OperationFailure("quota exceeded")

        with pytest.raises(QuotaError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_server_selection_timeout_raises_timeout_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = ServerSelectionTimeoutError("timeout")

        with pytest.raises(TimeoutError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)

    def test_auto_reconnect_raises_destination_error(self):
        uploader = _make_uploader()
        file_data = MagicMock()
        file_data.identifier = "test_id"
        collection = MagicMock()
        collection.delete_many.side_effect = AutoReconnect("connection lost")

        with pytest.raises(DestinationConnectionError):
            uploader.delete_by_record_id(collection=collection, file_data=file_data)
